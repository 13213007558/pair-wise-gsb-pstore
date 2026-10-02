# frozen_string_literal: true
require 'test/unit'
require 'pstore'
require 'tmpdir'
require 'fileutils'
require 'rbconfig'

class PStoreMultiTransactionTest < Test::Unit::TestCase
  LIB_DIR = File.expand_path("../lib", __dir__)

  # Child process: runs a coordinated transaction that writes
  # stock=9 / pending=9, optionally killing itself (Process.exit!) at a
  # persistent write boundary selected by the environment:
  #
  #   CRASH_POINT=in_block        die inside the block, before any write
  #   CRASH_AFTER_WRITES=1        die after the commit record is durable
  #   CRASH_AFTER_WRITES=2        die after the first store file is replaced
  #   CRASH_AFTER_WRITES=3        die after the second store file is replaced
  #   CRASH_POINT=before_cleanup  die after all writes, before journal cleanup
  WRITER_SCRIPT = <<~'RUBY'
    require "pstore"
    dir = ARGV[0]
    inventory = PStore.new(File.join(dir, "inventory.pstore"))
    reserved = PStore.new(File.join(dir, "reserved.pstore"))
    module CrashInjector
      def atomic_write(path, content)
        super
        $crash_writes = ($crash_writes || 0) + 1
        Process.exit!(0) if ENV["CRASH_AFTER_WRITES"].to_i == $crash_writes
      end
      def discard_journal(dir)
        Process.exit!(0) if ENV["CRASH_POINT"] == "before_cleanup"
        super
      end
    end
    PStore::MultiTransaction.prepend(CrashInjector)
    multi = PStore::MultiTransaction.new(inventory, reserved)
    multi.transaction do |tx|
      Process.exit!(0) if ENV["CRASH_POINT"] == "in_block"
      tx[inventory][:stock] = 9
      tx[reserved][:pending] = 9
    end
  RUBY

  # Child process: recovers (explicitly or implicitly) in a fresh process,
  # then prints the values of both stores.
  READER_SCRIPT = <<~'RUBY'
    require "pstore"
    dir = ARGV[0]
    inventory = PStore.new(File.join(dir, "inventory.pstore"))
    reserved = PStore.new(File.join(dir, "reserved.pstore"))
    multi = PStore::MultiTransaction.new(inventory, reserved)
    if ENV["IMPLICIT_RECOVER"]
      multi.transaction { |tx| tx } # recovering transaction
    else
      multi.recover
    end
    inventory.transaction(true) { puts "stock=#{inventory[:stock].inspect}" }
    reserved.transaction(true) { puts "pending=#{reserved[:pending].inspect}" }
  RUBY

  def setup
    @dir = Dir.mktmpdir("pstore-multi")
    @inventory_path = File.join(@dir, "inventory.pstore")
    @reserved_path = File.join(@dir, "reserved.pstore")
    @inventory = PStore.new(@inventory_path)
    @reserved = PStore.new(@reserved_path)
    seed(stock: 100, pending: 0)
  end

  def teardown
    FileUtils.remove_entry(@dir) if @dir && File.exist?(@dir)
  end

  def seed(stock:, pending:)
    @inventory.transaction { @inventory[:stock] = stock }
    @reserved.transaction { @reserved[:pending] = pending }
  end

  def run_child(script, env = {})
    output = IO.popen([env, RbConfig.ruby, "-I", LIB_DIR, "-e", script, @dir],
                      err: [:child, :out], &:read)
    [output, $?]
  end

  def crash_writer(env)
    run_child(WRITER_SCRIPT, env)
  end

  def recover_and_read(env = {})
    output, status = run_child(READER_SCRIPT, env)
    values = {}
    output.scan(/^(\w+)=(.*)$/) { |key, value| values[key] = value }
    [values, status, output]
  end

  def journal_dir
    File.join(@dir, ".pstore-multi-txn")
  end

  # --- basic coordinated commit -------------------------------------------

  def test_commit_updates_all_stores_atomically
    multi = PStore::MultiTransaction.new(@inventory, @reserved)
    multi.transaction do |tx|
      tx[@inventory][:stock] = 99
      tx[@reserved][:pending] = 1
    end
    @inventory.transaction(true) { assert_equal 99, @inventory[:stock] }
    @reserved.transaction(true) { assert_equal 1, @reserved[:pending] }
    assert_false File.exist?(journal_dir), "journal should be cleaned up"
  end

  def test_block_value_is_returned
    multi = PStore::MultiTransaction.new(@inventory, @reserved)
    assert_equal :ok, multi.transaction { :ok }
  end

  def test_exception_in_block_leaves_all_stores_untouched
    multi = PStore::MultiTransaction.new(@inventory, @reserved)
    assert_raise(RuntimeError) do
      multi.transaction do |tx|
        tx[@inventory][:stock] = 9
        tx[@reserved][:pending] = 9
        raise "boom"
      end
    end
    @inventory.transaction(true) { assert_equal 100, @inventory[:stock] }
    @reserved.transaction(true) { assert_equal 0, @reserved[:pending] }
    assert_false File.exist?(journal_dir)
  end

  def test_transaction_recovers_interrupted_commit_before_running
    multi = PStore::MultiTransaction.new(@inventory, @reserved)
    crash_writer("CRASH_AFTER_WRITES" => "2")
    multi.transaction do |tx|
      # The interrupted commit (stock=9) must already be applied.
      assert_equal 9, tx[@inventory][:stock]
      tx[@inventory][:stock] = 5
    end
    @inventory.transaction(true) { assert_equal 5, @inventory[:stock] }
    @reserved.transaction(true) { assert_equal 9, @reserved[:pending] }
  end

  def test_lock_order_is_fixed_regardless_of_store_order
    direct = PStore::MultiTransaction.new(@inventory, @reserved)
    reversed = PStore::MultiTransaction.new(@reserved, @inventory)
    assert_equal direct.paths, reversed.paths
    assert_equal [@inventory_path, @reserved_path].sort, direct.paths
  end

  def test_concurrent_coordinators_do_not_deadlock
    one = PStore::MultiTransaction.new(@inventory, @reserved)
    two = PStore::MultiTransaction.new(@reserved, @inventory)
    threads = [one, two].map do |multi|
      Thread.new do
        10.times do
          multi.transaction do |tx|
            tx[@inventory][:stock] = tx[@inventory][:stock].to_i + 1
            tx[@reserved][:pending] = tx[@reserved][:pending].to_i + 1
          end
        end
      end
    end
    assert_join_threads(threads)
    @inventory.transaction(true) { assert_equal 120, @inventory[:stock] }
    @reserved.transaction(true) { assert_equal 20, @reserved[:pending] }
  end

  def test_requires_at_least_two_distinct_stores
    assert_raise(PStore::MultiTransaction::Error) do
      PStore::MultiTransaction.new(@inventory)
    end
    assert_raise(PStore::MultiTransaction::Error) do
      PStore::MultiTransaction.new(@inventory, PStore.new(@inventory_path))
    end
  end

  def test_nested_multi_transaction_is_rejected
    multi = PStore::MultiTransaction.new(@inventory, @reserved)
    multi.transaction do
      assert_raise(PStore::MultiTransaction::Error) { multi.transaction {} }
    end
  end

  def test_unknown_store_in_block_is_rejected
    outsider = PStore.new(File.join(@dir, "outsider.pstore"))
    multi = PStore::MultiTransaction.new(@inventory, @reserved)
    assert_raise(PStore::MultiTransaction::Error) do
      multi.transaction { |tx| tx[outsider] }
    end
  end

  # --- crash recovery: new process recovers and reads ----------------------

  def test_crash_before_any_write_leaves_all_old_values
    crash_writer("CRASH_POINT" => "in_block")
    values, status, output = recover_and_read
    assert_predicate status, :success?, output
    assert_equal "100", values["stock"]
    assert_equal "0", values["pending"]
  end

  def test_crash_after_commit_record_recovers_all_new_values
    crash_writer("CRASH_AFTER_WRITES" => "1")
    values, status, output = recover_and_read
    assert_predicate status, :success?, output
    assert_equal "9", values["stock"]
    assert_equal "9", values["pending"]
    assert_false File.exist?(journal_dir), "recovery should remove the journal"
  end

  def test_crash_after_first_store_write_recovers_all_new_values
    crash_writer("CRASH_AFTER_WRITES" => "2")
    values, status, output = recover_and_read
    assert_predicate status, :success?, output
    assert_equal "9", values["stock"]
    assert_equal "9", values["pending"]
  end

  def test_crash_after_last_store_write_recovers_all_new_values
    crash_writer("CRASH_AFTER_WRITES" => "3")
    values, status, output = recover_and_read
    assert_predicate status, :success?, output
    assert_equal "9", values["stock"]
    assert_equal "9", values["pending"]
  end

  def test_crash_before_journal_cleanup_recovers_all_new_values
    crash_writer("CRASH_POINT" => "before_cleanup")
    values, status, output = recover_and_read
    assert_predicate status, :success?, output
    assert_equal "9", values["stock"]
    assert_equal "9", values["pending"]
    assert_false File.exist?(journal_dir)
  end

  def test_recovery_is_idempotent
    crash_writer("CRASH_AFTER_WRITES" => "2")
    2.times do
      values, status, output = recover_and_read
      assert_predicate status, :success?, output
      assert_equal "9", values["stock"]
      assert_equal "9", values["pending"]
    end
  end

  def test_implicit_recovery_by_next_transaction
    crash_writer("CRASH_AFTER_WRITES" => "1")
    values, status, output = recover_and_read("IMPLICIT_RECOVER" => "1")
    assert_predicate status, :success?, output
    assert_equal "9", values["stock"]
    assert_equal "9", values["pending"]
  end

  # --- damaged recovery material: stop and report, never guess -------------

  def plant_journal(record_content)
    dir = File.join(journal_dir, "pid.deadbeef")
    FileUtils.mkdir_p(dir)
    if record_content
      File.binwrite(File.join(dir, "commit.json"), record_content)
    end
    dir
  end

  def test_damaged_commit_record_stops_recovery_and_reports
    plant_journal("this is not json")
    _values, status, output = recover_and_read
    assert_false status.success?
    assert_include output, "RecoveryError"
    # Files must be untouched: recovery must not guess.
    values, status, = recover_and_read_after_cleanup
    assert_equal "100", values["stock"]
    assert_equal "0", values["pending"]
  end

  def test_missing_commit_record_stops_recovery_and_reports
    plant_journal(nil)
    _values, status, output = recover_and_read
    assert_false status.success?
    assert_include output, "RecoveryError"
  end

  def test_conflicting_journals_stop_recovery_and_report
    plant_journal("{}")
    other = File.join(journal_dir, "pid.cafebabe")
    FileUtils.mkdir_p(other)
    File.binwrite(File.join(other, "commit.json"), "{}")
    _values, status, output = recover_and_read
    assert_false status.success?
    assert_include output, "RecoveryError"
  end

  def test_record_for_wrong_participants_stops_recovery_and_reports
    record = JSON.generate(
      "version" => 1,
      "token" => "x",
      "participants" => [
        {"path" => @inventory_path, "content" => [Marshal.dump({})].pack("m0")},
        {"path" => File.join(@dir, "elsewhere.pstore"),
         "content" => [Marshal.dump({})].pack("m0")},
      ]
    )
    plant_journal(record)
    _values, status, output = recover_and_read
    assert_false status.success?
    assert_include output, "RecoveryError"
  end

  # Removes planted journals so a plain read can verify the store contents.
  def recover_and_read_after_cleanup
    FileUtils.rm_rf(journal_dir)
    recover_and_read
  end

  # --- single-store interface keeps working --------------------------------

  def test_single_store_interface_unaffected
    @inventory.transaction do
      @inventory[:stock] = 42
    end
    @inventory.transaction(true) do
      assert_equal 42, @inventory[:stock]
    end
    assert_false File.exist?(journal_dir)
  end
end
