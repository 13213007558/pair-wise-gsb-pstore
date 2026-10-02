# frozen_string_literal: true

require "fileutils"
require "json"
require "securerandom"

class PStore
  # PStore::MultiTransaction coordinates a single atomic commit across a
  # fixed set of PStore files.
  #
  # A plain PStore#transaction makes one file all-or-nothing, but two stores
  # committed separately can leave one file updated and the other not (for
  # example when the process dies between the two commits). This coordinator
  # adds a real, disk-based two-phase commit protocol on top:
  #
  # 1. The stores are locked in a fixed (sorted-path) order, so concurrent
  #    coordinators cannot deadlock.
  # 2. The block mutates in-memory copies of each store's data; nothing is
  #    written to the store files yet.
  # 3. A commit record is written to a journal file and fsynced. The record
  #    embeds the full new content of every participant, so the commit
  #    decision is self-describing and recoverable.
  # 4. Each participant file is atomically replaced (temp file + fsync +
  #    rename + directory fsync), in the same fixed order.
  # 5. The journal is removed.
  #
  # If the process dies after step 3, the commit decision survives. The next
  # call to #transaction (or MultiTransaction.recover) re-executes step 4
  # from the journal. Recovery is idempotent: applying the same recorded
  # content twice is a no-op, and it never guesses what a file should
  # contain -- it only replays what the fsynced record says.
  #
  # If the process dies before the journal is durable, every participant
  # still holds its old content, so the group is atomically "all old".
  #
  # Example:
  #
  #   inventory = PStore.new("inventory.pstore")
  #   reserved  = PStore.new("reserved.pstore")
  #   multi = PStore::MultiTransaction.new(inventory, reserved)
  #   multi.transaction do |tx|
  #     stock = tx[inventory][:stock]
  #     tx[inventory][:stock] = stock - 1
  #     tx[reserved][:pending] = (tx[reserved][:pending] || 0) + 1
  #   end
  #
  # Single-store PStore#transaction keeps working unchanged; it simply does
  # not participate in cross-file atomicity.
  class MultiTransaction
    # Raised for usage errors (duplicate stores, nested transactions, ...).
    class Error < PStore::Error
    end

    # Raised when recovery cannot be performed safely, for example when the
    # journal or a recorded commit record is missing or damaged. Recovery
    # never guesses at file contents, so this is a hard stop.
    class RecoveryError < Error
    end

    JOURNAL_DIRECTORY = ".pstore-multi-txn" # :nodoc:
    RECORD_FILE = "commit.json" # :nodoc:
    TEMP_SUFFIX = ".newtmp" # :nodoc:

    # Process-local locks keyed by canonical store path. flock(2) alone is
    # not enough on platforms (e.g. macOS) where it is emulated with
    # process-scoped fcntl locks, which do not exclude other threads of the
    # same process. These mutexes provide in-process mutual exclusion while
    # flock covers other processes. Both are always taken in the same fixed
    # (sorted-path) order, so they cannot deadlock.
    LOCAL_LOCKS = {} # :nodoc:
    LOCAL_LOCKS_GUARD = Thread::Mutex.new # :nodoc:
    private_constant :LOCAL_LOCKS, :LOCAL_LOCKS_GUARD

    # Creates a coordinator for the given PStore instances (two or more).
    # The set of participants is fixed for the lifetime of the object.
    def initialize(*stores)
      stores = stores.flatten
      raise Error, "at least two stores are required" if stores.size < 2
      stores.each do |store|
        unless store.is_a?(PStore)
          raise Error, "participants must be PStore instances"
        end
      end
      paths = stores.map { |store| File.expand_path(store.path) }
      if paths.uniq.size != paths.size
        raise Error, "each participant store must use a distinct file"
      end
      @participants = stores.each_with_index.map do |store, index|
        {store: store, path: paths[index]}
      end.sort_by { |entry| entry[:path] } # fixed lock/apply order
      @journal_dir = File.join(File.dirname(@participants.first[:path]),
                               JOURNAL_DIRECTORY)
      @txn_lock = Thread::Mutex.new
    end

    # Returns the absolute paths of the participant stores, in the fixed
    # (sorted) order used for locking and for applying commits.
    def paths
      @participants.map { |entry| entry[:path] }
    end

    # Runs a coordinated transaction across every participant store.
    #
    # The block receives a Transaction accessor; use tx[store] to get the
    # hash-like working copy for that store. When the block returns
    # normally, all stores are committed atomically (see the class
    # documentation). If the block raises, nothing is written.
    #
    # Before starting, any interrupted previous commit is recovered, so the
    # group is always observed as "all old" or "all new".
    def transaction
      raise Error, "nested multi-store transaction" unless @txn_lock.try_lock
      begin
        recover
        locks = acquire_locks
        begin
          working = {}
          @participants.each do |entry|
            working[entry[:store]] = read_table(entry[:path])
          end
          result = yield Transaction.new(working)
          journal = write_commit_record(working)
          begin
            apply_journal(journal)
          ensure
            discard_journal(journal)
          end
          result
        ensure
          release_locks(locks)
        end
      ensure
        @txn_lock.unlock
      end
    end

    # Replays a pending commit decision, if one was left behind by a process
    # that died mid-commit. Safe to call any number of times; does nothing
    # when no commit was in flight. Raises RecoveryError when recovery
    # material exists but is missing pieces or is damaged.
    def recover
      journal = find_journal
      return unless journal
      apply_journal(journal)
      discard_journal(journal)
      nil
    end

    private

    # Accessor handed to the transaction block. Hash-like per store.
    class Transaction
      def initialize(working) # :nodoc:
        @working = working
      end

      # Returns the working copy (a Hash) for the given participant store.
      def [](store)
        @working.fetch(store) do
          raise Error, "store #{store.path} is not a participant"
        end
      end
    end
    private_constant :Transaction

    def acquire_locks
      acquired = []
      @participants.each do |entry|
        mutex = LOCAL_LOCKS_GUARD.synchronize do
          LOCAL_LOCKS[entry[:path]] ||= Thread::Mutex.new
        end
        mutex.lock
        begin
          file = File.open(entry[:path], File::RDWR | File::CREAT | File::BINARY)
          begin
            file.flock(File::LOCK_EX)
          rescue Exception
            file.close
            raise
          end
          acquired << [mutex, file]
        rescue Exception
          mutex.unlock
          release_locks(acquired)
          raise
        end
      end
      acquired
    end

    def release_locks(locks)
      locks.reverse_each do |mutex, file|
        file.close
        mutex.unlock
      end
    end

    def read_table(path)
      data = File.binread(path)
      return {} if data.empty?
      table = Marshal.load(data)
      unless table.is_a?(Hash)
        raise Error, "PStore file #{path} seems to be corrupted."
      end
      table
    rescue EOFError, TypeError, ArgumentError
      raise Error, "PStore file #{path} seems to be corrupted."
    end

    def journal_record_path(dir)
      File.join(dir, RECORD_FILE)
    end

    # Writes the commit decision to a fresh journal directory and fsyncs it.
    # Once this returns, the transaction is committed in intent: recovery
    # will replay exactly this record.
    def write_commit_record(working)
      token = "#{Process.pid}.#{SecureRandom.hex(8)}"
      dir = File.join(@journal_dir, token)
      FileUtils.mkdir_p(dir)
      record = {
        "version" => 1,
        "token" => token,
        "participants" => @participants.map { |entry|
          {
            "path" => entry[:path],
            "content" => [Marshal.dump(working[entry[:store]])].pack("m0"),
          }
        },
      }
      atomic_write(journal_record_path(dir), JSON.generate(record))
      dir
    rescue Exception
      FileUtils.rm_rf(dir)
      raise
    end

    # Locates the pending commit record, if any. At most one journal
    # directory may exist; anything else is damage we refuse to guess about.
    def find_journal
      return nil unless File.directory?(@journal_dir)
      entries = Dir.children(@journal_dir).reject { |name| name.start_with?(".") }
      return nil if entries.empty?
      if entries.size > 1
        raise RecoveryError,
              "multiple commit journals in #{@journal_dir}; cannot decide which to apply"
      end
      File.join(@journal_dir, entries.first)
    end

    def load_journal(dir)
      path = journal_record_path(dir)
      unless File.file?(path)
        raise RecoveryError, "commit record #{path} is missing; cannot recover"
      end
      begin
        record = JSON.parse(File.binread(path))
      rescue JSON::ParserError
        raise RecoveryError, "commit record #{path} is damaged; cannot recover"
      end
      unless record.is_a?(Hash) &&
             record["version"] == 1 &&
             record["participants"].is_a?(Array) &&
             record["participants"].size == @participants.size
        raise RecoveryError, "commit record #{path} is malformed; cannot recover"
      end
      expected = @participants.map { |entry| entry[:path] }.sort
      recorded = record["participants"].map { |p| p.is_a?(Hash) ? p["path"] : nil }.sort
      unless recorded == expected
        raise RecoveryError,
              "commit record #{path} does not match the participant set; cannot recover"
      end
      record["participants"].map do |participant|
        content = participant["content"]
        begin
          decoded = content.is_a?(String) ? content.unpack1("m0") : nil
        rescue ArgumentError
          decoded = nil
        end
        unless decoded && valid_pstore_data?(decoded)
          raise RecoveryError,
                "commit record #{path} has damaged content for #{participant['path']}; cannot recover"
        end
        [participant["path"], decoded]
      end
    end

    def valid_pstore_data?(data)
      data = Marshal.load(data)
      data.is_a?(Hash)
    rescue EOFError, TypeError, ArgumentError
      false
    end

    # Applies a journaled commit to every participant, in the fixed order.
    # Idempotent: re-applying identical content changes nothing.
    def apply_journal(dir)
      load_journal(dir).each do |path, content|
        atomic_write(path, content)
      end
    end

    def discard_journal(dir)
      FileUtils.rm_rf(dir)
      begin
        Dir.rmdir(@journal_dir)
      rescue Errno::ENOENT, Errno::ENOTEMPTY, Errno::EACCES
        nil
      end
    end

    # Replaces +path+ with +content+ atomically: write to a temp file in the
    # same directory, fsync, rename over the target, fsync the directory.
    def atomic_write(path, content)
      dir = File.dirname(path)
      temp = File.join(dir, ".#{File.basename(path)}.#{Process.pid}.#{SecureRandom.hex(8)}#{TEMP_SUFFIX}")
      File.open(temp, File::WRONLY | File::CREAT | File::TRUNC | File::BINARY, 0o600) do |file|
        file.write(content)
        file.flush
        file.fsync
      end
      File.rename(temp, path)
      fsync_directory(dir)
    ensure
      File.unlink(temp) if temp && File.exist?(temp)
    end

    def fsync_directory(dir)
      File.open(dir, File::RDONLY) do |file|
        file.fsync
      end
    rescue SystemCallError, NotImplementedError
      nil
    end
  end
end
