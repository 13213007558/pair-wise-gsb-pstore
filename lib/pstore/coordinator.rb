# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "securerandom"

class PStore
  # Coordinates one transaction across a fixed group of PStore files.
  #
  # The coordinator locks the stores in a stable path order, writes a
  # checksummed manifest and prepared copies, durably records a COMMIT
  # decision, then renames the prepared copies over the original files.
  # Recovery is idempotent: with a COMMIT record it rolls forward to the new
  # contents; without one it rolls back to the original contents. Missing or
  # corrupt recovery material raises PStore::Error instead of inferring file
  # contents.
  class Coordinator
    MAGIC = "PST2PC1\n".b.freeze
    COMMIT_RECORD = "commit".b.freeze
    RECORD_DIGEST_SIZE = 64
    private_constant :MAGIC, :COMMIT_RECORD, :RECORD_DIGEST_SIZE

    State = Struct.new(:store, :identity, :path, :file, :table, :original_data,
                       :new_data, :prepared_path, keyword_init: true)
    private_constant :State

    # Creates a coordinator for a fixed array of store paths or PStore objects.
    #
    # Recovery material is stored in a hidden directory beside the first
    # sorted store path. Pass +journal_dir:+ to place that directory
    # elsewhere. The set of paths is part of the coordinator identity, so
    # recovery checks that the stored group matches the files requested by
    # the caller.
    def initialize(stores, journal_dir: nil)
      unless stores.is_a?(Array) && !stores.empty?
        raise PStore::Error, "at least one store is required"
      end

      @stores = stores.map { |store| coerce_store(store) }
      identities = @stores.map { |store| canonical_path(store.path) }
      if identities.uniq.size != identities.size
        raise PStore::Error, "stores must have distinct paths"
      end

      @identities = identities.sort.freeze
      @ordered_stores = @stores.sort_by { |store| canonical_path(store.path) }.freeze
      @root = configure_journal_dir(journal_dir)
      @root_lock = File.join(@root, "coordinator.lock")
    end

    attr_reader :root

    # Runs one transaction over all stores. The block receives the stores in
    # the same order supplied to the constructor, while locks are acquired in
    # canonical path order. Calling abort on any store aborts all stores.
    def transaction(read_only = false)
      states = nil
      states = open_store_states(read_only)
      begin
        ordered_by_caller = states.sort_by { |state| @stores.index(state.store) }
        run_blocks(ordered_by_caller, 0) { |*block_stores| yield(*block_stores) }
        if states.any? { |state| state.store.instance_variable_get(:@abort) }
          return nil
        end
        return nil if read_only

        prepare(states)
      ensure
        close_store_states(states || [])
      end
    end

    # Replays or cleans recovery material for this fixed store group. Returns
    # +true+ when material was processed and +false+ when recovery was already
    # complete. The method is safe to call repeatedly.
    def recover
      states = nil
      states = open_store_states(false)
      begin
        changed = false
        with_root_lock do
          changed = recover_locked
        end
        changed
      ensure
        close_store_states(states || [])
      end
    end

    private

    def coerce_store(store)
      return store if store.is_a?(PStore)

      PStore.new(store)
    end

    def configure_journal_dir(journal_dir)
      suffix = Digest::SHA512.hexdigest(@identities.join("\0"))[0, 32]
      if journal_dir
        File.join(File.expand_path(journal_dir), ".pstore-coordinator-#{suffix}")
      else
        File.join(File.dirname(@identities.first), ".pstore-coordinator-#{suffix}")
      end
    end

    def expanded_path(path)
      File.expand_path(path)
    end

    def canonical_path(path)
      expanded = expanded_path(path)
      File.realpath(expanded)
    rescue Errno::ENOENT, Errno::ENOTDIR
      expanded
    end

    def open_store_states(read_only)
      states = []
      @ordered_stores.each do |store|
        file = store.send(:begin_coordinated_transaction, read_only)
        begin
          if file
            table, original_data = store.send(:load_coordinated_table, file)
          else
            table = {}
            original_data = nil
          end
          store.instance_variable_set(:@table, table)
          states << State.new(store: store, identity: canonical_path(store.path),
                              path: expanded_path(store.path), file: file, table: table,
                              original_data: original_data)
        rescue Exception
          store.send(:end_coordinated_transaction, file) if file
          close_store_states(states)
          raise
        end
      end
      states
    end

    def close_store_states(states)
      states.reverse_each do |state|
        state.store.send(:end_coordinated_transaction, state.file) if state.file
      end
    end

    def run_blocks(states, index, &block)
      return yield(*states.map(&:store)) if index == states.size

      state = states[index]
      value = nil
      caught = catch(state.store) do
        value = run_blocks(states, index + 1, &block)
        :completed
      end
      return nil if caught != :completed

      value
    end

    def prepare(states)
      states.each do |state|
        state.new_data = state.store.send(:dump, state.table)
      end

      with_root_lock do
        recover_locked

        transaction_id = "tx-#{Process.pid}-#{Time.now.to_i}-#{Time.now.nsec}-#{SecureRandom.hex(16)}"
        transaction_dir = File.join(@root, transaction_id)
        durable_mkdir(transaction_dir)

        states.each do |state|
          state.prepared_path = "#{state.path}.pstore-tx.#{Process.pid}.#{SecureRandom.hex(8)}"
        end
        states.each do |state|
          write_prepared_file(state)
        end

        manifest = build_manifest(states)
        write_record(File.join(transaction_dir, "MANIFEST"), "manifest", JSON.dump(manifest))
        fsync_directory(@root)
        manifest_payload = JSON.dump(manifest)
        write_record(File.join(transaction_dir, "COMMIT"), COMMIT_RECORD,
                     Digest::SHA512.digest(manifest_payload))
        fsync_directory(@root)
        apply_transaction(manifest["stores"])
        finish_transaction_dir(transaction_dir)
        fsync_directory(@root)
      end
    end

    def build_manifest(states)
      {
        "magic" => MAGIC,
        "version" => 1,
        "group" => @identities,
        "stores" => states.map do |state|
          {
            "path" => state.path,
            "identity" => state.identity,
            "old_digest" => Digest::SHA512.base64digest(state.original_data),
            "prepared_path" => state.prepared_path,
            "prepared_digest" => Digest::SHA512.base64digest(state.new_data),
            "prepared_size" => state.new_data.bytesize
          }
        end
      }
    end

    def write_prepared_file(state)
      mode = File.exist?(state.path) ? File.stat(state.path).mode : 0o600
      File.open(state.prepared_path,
                File::RDWR | File::CREAT | File::EXCL | File::BINARY,
                0o000, encoding: Encoding::ASCII_8BIT) do |file|
        file.write(state.new_data)
        file.flush
        file.chmod(mode)
        file.fsync
      end
      fsync_directory(File.dirname(state.prepared_path))
    rescue Exception
      begin
        File.unlink(state.prepared_path)
        fsync_directory(File.dirname(state.prepared_path))
      rescue SystemCallError
      end
      raise
    end

    def with_root_lock
      ensure_journal_root
      File.open(@root_lock, File::RDWR | File::CREAT | File::BINARY, 0o600,
                encoding: Encoding::ASCII_8BIT) do |file|
        file.flock(File::LOCK_EX)
        yield
      end
    end

    def ensure_journal_root
      FileUtils.mkdir_p(@root, mode: 0o700)
      path = @root
      while path != File.dirname(path)
        fsync_directory(File.dirname(path))
        path = File.dirname(path)
      end
    end

    def recover_locked
      changed = false
      Dir.children(@root).sort.each do |name|
        next if name == "coordinator.lock"

        path = File.join(@root, name)
        if name.start_with?("tx-")
          unless File.directory?(path)
            raise PStore::Error, "invalid recovery material: #{name}"
          end
          recover_transaction(path)
          changed = true
        elsif name.start_with?("removing-")
          unless File.directory?(path)
            raise PStore::Error, "invalid recovery material: #{name}"
          end
          remove_cleanup_dir(path)
          fsync_directory(@root)
          changed = true
        else
          raise PStore::Error, "unrecognized recovery material: #{name}"
        end
      end
      fsync_directory(@root) if changed
      changed
    end

    def recover_transaction(transaction_dir)
      entries = Dir.children(transaction_dir).sort
      invalid = entries - ["MANIFEST", "COMMIT"]
      unless invalid.empty?
        raise PStore::Error,
              "invalid recovery material in #{File.basename(transaction_dir)}: #{invalid.join(", ")}"
      end

      manifest_path = File.join(transaction_dir, "MANIFEST")
      unless File.exist?(manifest_path)
        roll_back_prepared_files
        finish_transaction_dir(transaction_dir)
        fsync_directory(@root)
        return
      end

      manifest_payload = read_record(manifest_path, "manifest")
      manifest = parse_manifest(manifest_payload)

      commit_path = File.join(transaction_dir, "COMMIT")
      if File.exist?(commit_path)
        decision = read_record(commit_path, COMMIT_RECORD)
        unless decision == Digest::SHA512.digest(manifest_payload)
          raise PStore::Error,
                "commit decision does not match manifest: #{File.basename(transaction_dir)}"
        end
        roll_forward(manifest["stores"])
      else
        roll_back(manifest["stores"])
      end

      finish_transaction_dir(transaction_dir)
      fsync_directory(@root)
    end

    def parse_manifest(payload)
      manifest = JSON.parse(payload)
      stores = manifest["stores"]
      unless manifest.is_a?(Hash) && manifest["magic"] == MAGIC &&
             manifest["version"] == 1 && manifest["group"] == @identities &&
             stores.is_a?(Array) && stores.length == @identities.length
        raise PStore::Error, "invalid transaction manifest"
      end

      stores.each do |entry|
        unless valid_manifest_store?(entry)
          raise PStore::Error, "invalid store entry in transaction manifest"
        end
      end
      manifest
    rescue JSON::ParserError
      raise PStore::Error, "invalid transaction manifest"
    end

    def valid_manifest_store?(entry)
      entry.is_a?(Hash) &&
        entry["path"].is_a?(String) &&
        entry["identity"].is_a?(String) &&
        entry["old_digest"].is_a?(String) &&
        entry["prepared_path"].is_a?(String) &&
        entry["prepared_digest"].is_a?(String) &&
        entry["prepared_size"].is_a?(Integer) &&
        canonical_path(entry["path"]) == entry["identity"] &&
        @identities.include?(entry["identity"]) &&
        valid_prepared_path?(entry["path"], entry["prepared_path"])
    end

    def valid_prepared_path?(store_path, prepared_path)
      File.dirname(prepared_path) == File.dirname(store_path) &&
        File.basename(prepared_path).start_with?("#{File.basename(store_path)}.pstore-tx.")
    end

    def roll_back(stores)
      stores.each do |entry|
        prepared_path = entry["prepared_path"]
        unless File.file?(prepared_path)
          raise PStore::Error, "missing uncommitted prepared file: #{prepared_path}"
        end

        data = File.binread(prepared_path)
        unless data.bytesize == entry["prepared_size"] &&
               Digest::SHA512.base64digest(data) == entry["prepared_digest"]
          raise PStore::Error, "corrupt uncommitted prepared file: #{prepared_path}"
        end

        File.unlink(prepared_path)
        fsync_directory(File.dirname(prepared_path))
      end
    end

    def roll_back_prepared_files
      @ordered_stores.each do |store|
        path = expanded_path(store.path)
        Dir.glob("#{path}.pstore-tx.*").sort.each do |prepared_path|
          File.unlink(prepared_path)
          fsync_directory(File.dirname(prepared_path))
        end
      end
    end

    def roll_forward(stores)
      stores.each do |entry|
        path = entry["path"]
        current_digest = current_digest_for(path)
        next if current_digest == entry["prepared_digest"]
        unless current_digest == entry["old_digest"]
          raise PStore::Error, "store diverged from recovery record: #{path}"
        end

        prepared_path = entry["prepared_path"]
        unless File.file?(prepared_path)
          raise PStore::Error, "missing committed prepared file: #{prepared_path}"
        end
        data = File.binread(prepared_path)
        unless data.bytesize == entry["prepared_size"] &&
               Digest::SHA512.base64digest(data) == entry["prepared_digest"]
          raise PStore::Error, "corrupt committed prepared file: #{prepared_path}"
        end

        durable_rename(prepared_path, path)
      end
    end

    def current_digest_for(path)
      Digest::SHA512.base64digest(File.file?(path) ? File.binread(path) : "")
    end

    def apply_transaction(stores)
      stores.each do |entry|
        unless current_digest_for(entry["path"]) == entry["old_digest"]
          raise PStore::Error, "store diverged before commit: #{entry["path"]}"
        end

        durable_rename(entry["prepared_path"], entry["path"])
      end
    end

    def finish_transaction_dir(transaction_dir)
      cleanup_path = File.join(@root, "removing-#{File.basename(transaction_dir)}")
      File.rename(transaction_dir, cleanup_path)
      fsync_directory(@root)
      remove_cleanup_dir(cleanup_path)
      fsync_directory(@root)
    end

    def remove_cleanup_dir(path)
      FileUtils.rm_rf(path)
    end

    def durable_rename(source, destination)
      File.rename(source, destination)
      fsync_directory(File.dirname(destination))
    end

    def write_record(path, type, payload)
      payload = String(payload).b
      content = MAGIC + [type].pack("a8") + [payload.bytesize].pack("Q>") +
                payload + Digest::SHA512.digest(payload)
      File.open(path, File::RDWR | File::CREAT | File::EXCL | File::BINARY, 0o600,
                encoding: Encoding::ASCII_8BIT) do |file|
        file.write(content)
        file.flush
        file.fsync
      end
      fsync_directory(File.dirname(path))
    end

    def read_record(path, expected_type)
      raise PStore::Error, "missing recovery material: #{path}" unless File.file?(path)

      content = File.binread(path)
      minimum_size = MAGIC.bytesize + 16 + RECORD_DIGEST_SIZE
      if content.bytesize < minimum_size || content.byteslice(0, MAGIC.bytesize) != MAGIC
        raise PStore::Error, "corrupt recovery record: #{path}"
      end

      type = content.byteslice(MAGIC.bytesize, 8)
      size = content.byteslice(MAGIC.bytesize + 8, 8).unpack1("Q>")
      payload_start = MAGIC.bytesize + 16
      payload_end = payload_start + size
      unless type == [expected_type].pack("a8") &&
             content.bytesize == payload_end + RECORD_DIGEST_SIZE
        raise PStore::Error, "corrupt recovery record: #{path}"
      end

      payload = content.byteslice(payload_start, size)
      digest = content.byteslice(payload_end, RECORD_DIGEST_SIZE)
      unless Digest::SHA512.digest(payload) == digest
        raise PStore::Error, "corrupt recovery record: #{path}"
      end
      payload
    end

    def durable_mkdir(path)
      Dir.mkdir(path, 0o700)
      fsync_directory(File.dirname(path))
    end

    def fsync_directory(path)
      return if Gem.win_platform?

      File.open(path, File::RDONLY | File::BINARY, encoding: Encoding::ASCII_8BIT) do |directory|
        directory.fsync
      end
    rescue Errno::EINVAL, Errno::ENOTSUP
      nil
    end
  end
end
