defmodule DeduplicatorTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  @chunk_dir "test/chunks"
  @recovery_dir "test/recovered"
  @resources_dir "test/resources"

  setup_all do
    File.mkdir(@chunk_dir)
    File.mkdir(@recovery_dir)
    on_exit(fn -> File.rm_rf!(@chunk_dir) end)
    :ok
  end

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Deduplicator.Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Deduplicator.Repo, {:shared, self()})
    :ok
  end

  @bytes Application.get_env(:deduplicator, :chuck_size_bytes)

  test "Read bytes from file" do
    [
      @resources_dir <> "/text.txt",
      @resources_dir <> "/pdf_example.pdf"
    ]
    |> Enum.each(fn filename ->
      %{size: initial_size} = File.stat!(filename)

      res = Deduplicator.Files.read(filename, @bytes)

      res
      |> Enum.each(&assert(byte_size(&1) <= @bytes))

      IO.puts("Chunk amount #{Enum.count(res)}")

      read_size =
        res
        |> Enum.reduce(0, &(byte_size(&1) + &2))
        |> IO.inspect(label: "Byte size")

      assert initial_size == read_size
    end)
  end

  describe "Calculate hash for chucks" do
    setup do
      chunks =
        @resources_dir <> "/text.txt"
        |> Deduplicator.Files.read(@bytes)
      {:ok, %{chunks: chunks}}
    end

    test "with sha", %{chunks: chunks} do
      chunks
      |> Enum.each(&Deduplicator.Hash.binary_hash(&1, :sha))
    end

    test "with md5", %{chunks: chunks} do
      chunks
      |> Enum.each(&Deduplicator.Hash.binary_hash(&1, :md5))
    end
  end

  describe "Read & save chunks" do
    test "txt" do
      input_file = @resources_dir <> "/text.txt"
      print_file_size(input_file)

      {:ok, output_file} =
        input_file
        |> Deduplicator.deduplicate_file(output_filepath: @chunk_dir)

      chunk_repetition()

      print_file_size(output_file)
    end

    test "pdf" do
      input_file = @resources_dir <> "/pdf_example.pdf"
      print_file_size(input_file)

      {:ok, output_file} =
        input_file
        |> Deduplicator.deduplicate_file(output_filepath: @chunk_dir, bytes: 32, chunk_amount: 1000)

      chunk_repetition()

      print_file_size(output_file)
    end
  end

  describe "Recover file" do
    test "txt" do
      input_file = @resources_dir <> "/text.txt"
      print_file_size(input_file)

      {:ok, output_file} =
        input_file
        |> Deduplicator.deduplicate_file(output_filepath: @chunk_dir)

      print_file_size(output_file)

      recovered_file = @recovery_dir <> "/text.txt"
      {:ok, _} = Deduplicator.recovery_file(output_file, recovered_file)
      print_file_size(recovered_file)

      assert_file_equals(input_file, recovered_file)
    end

    test "pdf" do
      input_file = @resources_dir <> "/pdf_example.pdf"
      print_file_size(input_file)

      {:ok, output_file} =
        input_file
        |> Deduplicator.deduplicate_file(output_filepath: @chunk_dir, bytes: 32)

      print_file_size(output_file)

      chunk_repetition()

      recovered_file = @recovery_dir <> "/pdf_example.pdf"
      {:ok, _} = Deduplicator.recovery_file(output_file, recovered_file)
      print_file_size(recovered_file)

      assert_file_equals(input_file, recovered_file)
    end

    @tag :skip
    @tag timeout: 400_000
    test "jpg" do
      bytes = 20
      input_file = @resources_dir <> "/IMG_0036.jpg"
      print_file_size(input_file)

      count =
        input_file
        |> Deduplicator.Files.read(bytes)
        |> Enum.count()

      IO.puts("Chunk amount #{count}")

      {:ok, output_file} =
        input_file
        |> Deduplicator.deduplicate_file(output_filepath: @chunk_dir, bytes: bytes, chunk_amount: 1000)

      chunk_repetition()

      print_file_size(output_file)

      recovered_file = @recovery_dir <> "/IMG_0036.jpg"
      {:ok, _} = Deduplicator.recovery_file(output_file, recovered_file)
      print_file_size(recovered_file)

      assert_file_equals(input_file, recovered_file)
    end
  end

  describe "Performance" do
    @performance_file "/pdf_example.pdf"
    @performance_bytes 16

    test "database with single insert" do
      filename = @resources_dir <> @performance_file
      {:ok, %{id: file_id}} = Deduplicator.Files.save_file(
        filename,
        @performance_bytes,
        Deduplicator.Hash.default_algorithm()
      )

      changesets =
        Deduplicator.Files.read(filename, @performance_bytes)
        |> Enum.map(fn chunk ->
          hash = Deduplicator.Hash.binary_hash(chunk)
          %{
            hash: hash,
            file_id: file_id,
            line: 100
          }
          |> Deduplicator.Schemas.HashLink.create_changeset()
        end)

      {time, _} = :timer.tc(fn ->
        Enum.each(changesets, &Deduplicator.Repo.insert(&1, on_conflict: :nothing))
      end)

      IO.puts("Insert one by one take #{time / 1000} ms")
    end

    test "database with insert all" do
      filename = @resources_dir <> @performance_file
      {:ok, %{id: file_id}} = Deduplicator.Files.save_file(
        filename,
        @performance_bytes,
        Deduplicator.Hash.default_algorithm()
      )
      data =
        Deduplicator.Files.read(filename, @performance_bytes)
        |> Enum.map(fn chunk ->
          hash = Deduplicator.Hash.binary_hash(chunk)
          %{
            hash: hash,
            file_id: file_id,
            line: 100
          }
        end)

      {time, _} = :timer.tc(fn ->
        Deduplicator.Repo.insert_all(Deduplicator.Schemas.HashLink, data, on_conflict: :nothing)
      end)

      IO.puts("Insert all take #{time / 1000} ms")
    end

    test "file with single insert" do
      filename = @resources_dir <> @performance_file
      output_file = @chunk_dir <> "/file_performance.txt"
      :ok = File.touch(output_file)
      {:ok, file} = File.open(output_file)

      changesets =
        Deduplicator.Files.read(filename, @performance_bytes)
        |> Enum.map(fn chunk ->
          Deduplicator.Hash.binary_hash(chunk)
        end)

      {time, _} = :timer.tc(fn ->
        Enum.each(changesets, &IO.binwrite(file, &1))
      end)

      IO.puts("Write one by one take #{time / 1000} ms")
    end

    test "file with insert all" do
      filename = @resources_dir <> @performance_file
      output_file = @chunk_dir <> "/file_performance.txt"
      :ok = File.touch(output_file)
      {:ok, file} = File.open(output_file)

      data =
        Deduplicator.Files.read(filename, @performance_bytes)
        |> Enum.map(fn chunk ->
          Deduplicator.Hash.binary_hash(chunk)
        end)
        |> Enum.join("")

      {time, _} = :timer.tc(fn ->
        IO.binwrite(file, data)
      end)

      IO.puts("Write all take #{time / 1000} ms")
    end
  end

  def print_file_size(filename) do
    %{size: size} = File.stat!(filename)
    IO.puts("File #{filename} size: #{size} bytes")
  end

  def assert_file_equals(file1, file2) do
    res1 = Deduplicator.Files.read(file1, 512) |> Enum.to_list()
    res2 = Deduplicator.Files.read(file2, 512) |> Enum.to_list()
    assert res1 == res2
  end

  def chunk_repetition do
    Deduplicator.Schemas.HashLink
    |> where([h], h.refs_num > 1)
    |> select([h], h.refs_num)
    |> order_by([h], desc: h.refs_num)
    |> Deduplicator.Repo.all()
    |> IO.inspect(label: "Chunk repetition")
  end
end
