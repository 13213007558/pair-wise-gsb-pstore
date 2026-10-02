# frozen_string_literal: true

require "test/unit"
require "pstore"
require "tmpdir"
require "json"
require "rbconfig"
require "open3"

class PStoreCoordinatorTest < Test::Unit::TestCase
  WORKER = <<~'RUBY'
    require "json"
    require "pstore"

    work = JSON.parse(ENV.fetch("WORK"))
    inventory_path = work.fetch("inventory")
    reservation_path = work.fetch("reservation")
    phase = ENV.fetch("PHASE", "")

    coordinator_class = Class.new(PStore::Coordinator)
    coordinator_class.prepend(Module.new do
      define_method(:ensure_journal_root) do
        result = super()
        Process.exit!(true) if phase == "root"
        result
      end

      define_method(:durable_mkdir) do |path|
        result = super(path)
        Process.exit!(true) if phase == "transaction_dir"
        result
      end

      define_method(:write_record) do |path, type, payload|
        result = super(path, type, payload)
        Process.exit!(true) if (phase == "manifest" && File.basename(path) == "MANIFEST") ||
                               (phase == "commit" && File.basename(path) == "COMMIT")
        result
      end

      define_method(:write_prepared_file) do |state|
        $prepared_fsync_state = state if phase == "prepared_inventory_fsync"
        result = super(state)
        Process.exit!(true) if phase == "prepared_inventory" && File.basename(state.path) == "inventory" ||
                               phase == "prepared_reservation" && File.basename(state.path) == "reservation"
        result
      end

      define_method(:durable_rename) do |source, destination|
        result = super(source, destination)
        Process.exit!(true) if (phase == "rename_inventory" && File.basename(destination) == "inventory") ||
                               (phase == "rename_reservation" && File.basename(destination) == "reservation")
        result
      end

      define_method(:finish_transaction_dir) do |path|
        cleanup_path = File.join(@root, "removing-#{File.basename(path)}")
        File.rename(path, cleanup_path)
        Process.exit!(true) if phase == "cleanup_remove"
        fsync_directory(@root)
        Process.exit!(true) if phase == "cleanup_rename_fsync"
        remove_cleanup_dir(cleanup_path)
        fsync_directory(@root)
        nil
      end

      define_method(:remove_cleanup_dir) do |path|
        result = super(path)
        Process.exit!(true) if phase == "cleanup_deleted" || phase == "cleanup_fsync"
        result
      end

      define_method(:fsync_directory) do |path|
        result = super(path)
        if phase == "prepared_inventory_fsync" && $prepared_fsync_state &&
           File.basename($prepared_fsync_state.path) == "inventory" &&
           path == File.dirname(inventory_path)
          Process.exit!(true)
        end
        Process.exit!(true) if phase == "cleanup_rename_fsync" && $cleaning_transaction && path == @root
        Process.exit!(true) if phase == "cleanup_fsync" && $cleaning_deleted && path == @root
        result
      end
    end)

    coordinator = coordinator_class.new([PStore.new(inventory_path), PStore.new(reservation_path)])

    case ENV.fetch("MODE")
    when "seed"
      coordinator.transaction do |inventory, reservations|
        inventory[:count] = 0
        reservations[:ids] = []
      end
    when "crash"
      coordinator.transaction do |inventory, reservations|
        inventory[:count] = 1
        reservations[:ids] = ["seat-1"]
      end
    when "recover"
      first = coordinator.recover
      second = coordinator.recover
      puts JSON.generate("first" => first, "second" => second)
    when "read"
      inventory = PStore.new(inventory_path)
      reservations = PStore.new(reservation_path)
      values = {
        "count" => inventory.transaction(true) { inventory[:count] },
        "ids" => reservations.transaction(true) { reservations[:ids] }
      }
      puts JSON.generate(values)
    end
  RUBY

  def test_coordinator_commits_all_stores
    in_temp_dir do |dir|
      inventory = PStore.new(File.join(dir, "inventory"))
      reservations = PStore.new(File.join(dir, "reservation"))
      coordinator = PStore::Coordinator.new([inventory, reservations])

      coordinator.transaction do |inventory_store, reservation_store|
        inventory_store[:count] = 1
        reservation_store[:ids] = ["seat-1"]
      end

      assert_equal(1, inventory.transaction(true) { inventory[:count] })
      assert_equal(["seat-1"], reservations.transaction(true) { reservations[:ids] })
      assert_equal(false, coordinator.recover)
    end
  end

  def test_coordinator_aborts_all_stores
    in_temp_dir do |dir|
      inventory = PStore.new(File.join(dir, "inventory"))
      reservations = PStore.new(File.join(dir, "reservation"))
      coordinator = PStore::Coordinator.new([inventory, reservations])
      coordinator.transaction do |inventory_store, reservation_store|
        inventory_store[:count] = 1
        reservation_store[:ids] = ["seat-1"]
      end

      coordinator.transaction do |inventory_store, reservation_store|
        inventory_store[:count] = 2
        reservation_store[:ids] = ["seat-2"]
        inventory_store.abort
      end

      assert_equal(1, inventory.transaction(true) { inventory[:count] })
      assert_equal(["seat-1"], reservations.transaction(true) { reservations[:ids] })
    end
  end

  def test_coordinator_rejects_writes_in_read_only_transaction
    in_temp_dir do |dir|
      inventory = PStore.new(File.join(dir, "inventory"))
      reservations = PStore.new(File.join(dir, "reservation"))
      coordinator = PStore::Coordinator.new([inventory, reservations])

      assert_raise(PStore::Error) do
        coordinator.transaction(true) { |inventory_store, _| inventory_store[:count] = 1 }
      end
    end
  end

  def test_coordinator_accepts_path_names_and_preserves_caller_order
    in_temp_dir do |dir|
      inventory_path = File.join(dir, "inventory")
      reservation_path = File.join(dir, "reservation")
      coordinator = PStore::Coordinator.new([reservation_path, inventory_path])
      received = nil

      coordinator.transaction do |reservations, inventory|
        received = [reservations.path, inventory.path]
        inventory[:count] = 3
        reservations[:ids] = ["a", "b", "c"]
      end

      assert_equal([reservation_path, inventory_path], received)
      assert_equal(3, PStore.new(inventory_path).transaction(true) { _1[:count] })
      assert_equal(["a", "b", "c"], PStore.new(reservation_path).transaction(true) { _1[:ids] })
    end
  end

  def test_read_only_coordinator_can_open_missing_files
    in_temp_dir do |dir|
      coordinator = PStore::Coordinator.new([File.join(dir, "inventory"),
                                             File.join(dir, "reservation")])

      coordinator.transaction(true) do |inventory, reservations|
        assert_nil(inventory[:count])
        assert_nil(reservations[:ids])
      end
    end
  end

  def test_explicit_journal_directory_is_group_scoped
    in_temp_dir do |dir|
      journal_base = File.join(dir, "journal")
      coordinator = PStore::Coordinator.new(
        [File.join(dir, "inventory"), File.join(dir, "reservation")],
        journal_dir: journal_base
      )
      coordinator.transaction do |inventory, reservations|
        inventory[:count] = 1
        reservations[:ids] = ["seat-1"]
      end

      assert(File.directory?(coordinator.root))
      assert(coordinator.root.start_with?(File.join(journal_base, ".pstore-coordinator-")))
      assert_equal(false, coordinator.recover)
    end
  end

  def test_rejects_duplicate_store_files
    in_temp_dir do |dir|
      path = File.join(dir, "inventory")
      assert_raise(PStore::Error) do
        PStore::Coordinator.new([path, File.join(dir, "inventory")])
      end
    end
  end

  {
    "root" => :old_no_recovery,
    "transaction_dir" => :old,
    "manifest" => :old,
    "prepared_inventory" => :old,
    "prepared_inventory_fsync" => :old,
    "prepared_reservation" => :old,
    "commit" => :new,
    "rename_inventory" => :new,
    "rename_reservation" => :new,
    "cleanup_remove" => :new,
    "cleanup_rename_fsync" => :new,
    "cleanup_deleted" => :new_no_recovery,
    "cleanup_fsync" => :new_no_recovery
  }.each do |phase, expectation|
    define_method("test_process_exit_at_#{phase}_is_recovered_to_#{expectation}") do
      in_temp_dir do |dir|
        run_worker(dir, "seed")
        run_worker(dir, "crash", phase: phase)

        if expectation == :error
          assert_raise(RuntimeError) { run_worker(dir, "recover") }
        else
          recovery = run_worker(dir, "recover")
          expected_changed = !expectation.end_with?("_no_recovery")
          assert_equal(expected_changed, recovery.fetch("first"))
          assert_equal(false, recovery.fetch("second"))
          assert_state(dir, expectation.to_s.sub("_no_recovery", "").to_sym)
        end
      end
    end
  end

  def test_corrupt_manifest_stops_recovery
    in_temp_dir do |dir|
      prepare_precommit_material(dir)
      path = File.join(transaction_dir(dir), "MANIFEST")
      File.binwrite(path, File.binread(path) + "x")

      assert_raise(RuntimeError) { run_worker(dir, "recover") }
      assert_state(dir, :old)
    end
  end

  def test_corrupt_uncommitted_prepared_file_stops_recovery
    in_temp_dir do |dir|
      prepare_precommit_material(dir)
      path = Dir.glob(File.join(dir, "reservation.pstore-tx.*")).first
      File.binwrite(path, File.binread(path) + "x")

      assert_raise(RuntimeError) { run_worker(dir, "recover") }
      assert_state(dir, :old)
    end
  end

  def test_missing_uncommitted_prepared_file_stops_recovery
    in_temp_dir do |dir|
      prepare_precommit_material(dir)
      File.unlink(Dir.glob(File.join(dir, "inventory.pstore-tx.*")).first)

      assert_raise(RuntimeError) { run_worker(dir, "recover") }
      assert_state(dir, :old)
    end
  end

  def test_missing_committed_prepared_file_stops_recovery
    in_temp_dir do |dir|
      prepare_committed_material(dir)
      File.unlink(Dir.glob(File.join(dir, "reservation.pstore-tx.*")).first)

      assert_raise(RuntimeError) { run_worker(dir, "recover") }
    end
  end

  def test_corrupt_committed_prepared_file_stops_recovery
    in_temp_dir do |dir|
      prepare_committed_material(dir)
      path = Dir.glob(File.join(dir, "reservation.pstore-tx.*")).first
      File.binwrite(path, File.binread(path) + "x")

      assert_raise(RuntimeError) { run_worker(dir, "recover") }
    end
  end

  def test_corrupt_commit_decision_stops_recovery
    in_temp_dir do |dir|
      prepare_committed_material(dir)
      path = File.join(transaction_dir(dir), "COMMIT")
      File.binwrite(path, File.binread(path) + "x")

      assert_raise(RuntimeError) { run_worker(dir, "recover") }
    end
  end

  def test_diverged_store_stops_recovery
    in_temp_dir do |dir|
      prepare_committed_material(dir)
      PStore.new(File.join(dir, "reservation")).transaction do |reservations|
        reservations[:ids] = ["unexpected"]
      end

      assert_raise(RuntimeError) { run_worker(dir, "recover") }
    end
  end

    def test_crash_during_roll_forward_is_recovered_on_retry
      in_temp_dir do |dir|
        run_worker(dir, "seed")
        run_worker(dir, "crash", phase: "commit")
        run_worker(dir, "recover", phase: "rename_inventory")

        recovery = run_worker(dir, "recover")
      assert_equal(true, recovery.fetch("first"))
      assert_equal(false, recovery.fetch("second"))
      assert_state(dir, :new)
    end
  end

    def test_crash_during_recovery_cleanup_is_safe_on_retry
      in_temp_dir do |dir|
        prepare_committed_material(dir)
        run_worker(dir, "recover", phase: "cleanup_remove")

      recovery = run_worker(dir, "recover")
      assert_equal(false, recovery.fetch("second"))
      assert_state(dir, :new)
    end
  end

  private

  def in_temp_dir
    Dir.mktmpdir("pstore-coordinator-") do |dir|
      yield File.realpath(dir)
    end
  end

  def prepare_precommit_material(dir)
    run_worker(dir, "seed")
    run_worker(dir, "crash", phase: "manifest")
  end

  def prepare_committed_material(dir)
    run_worker(dir, "seed")
    run_worker(dir, "crash", phase: "rename_inventory")
  end

  def transaction_dir(dir)
    journal_dir = Dir.children(dir).find { |name| name.start_with?(".pstore-coordinator-") }
    path = File.join(dir, journal_dir)
    Dir.children(path).map { |name| File.join(path, name) }.find { |child| File.directory?(child) }
  end

  def assert_state(dir, state)
    values = run_worker(dir, "read")
    if state == :old
      assert_equal(0, values.fetch("count"))
      assert_equal([], values.fetch("ids"))
    else
      assert_equal(1, values.fetch("count"))
      assert_equal(["seat-1"], values.fetch("ids"))
    end
  end

  def run_worker(dir, mode, phase: nil)
    work = {
      inventory: File.join(dir, "inventory"),
      reservation: File.join(dir, "reservation")
    }
    stdout, stderr, status = Open3.capture3(
      {
        "WORK" => JSON.generate(work),
        "MODE" => mode,
        "PHASE" => phase.to_s
      },
      RbConfig.ruby,
      "-I#{File.expand_path("../lib", __dir__)}",
      "-e", WORKER,
      chdir: dir
    )

    unless status.success?
      raise "worker #{mode} #{phase} failed: #{stderr.strip}"
    end
    return nil if stdout.empty?

    JSON.parse(stdout, exception: true)
  end
end
