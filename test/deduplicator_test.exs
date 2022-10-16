defmodule DeduplicatorTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  @test_dir "test/output"
  @resources_dir "test/resources"

  setup_all do
    File.mkdir(@test_dir)
#    on_exit(fn -> File.rm_rf!(@test_dir) end)
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

      res = Deduplicator.Files.Reader.read(filename)

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
        |> Deduplicator.Files.Reader.read(@bytes)
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

  test "Read & save chunks" do
    input_file = @resources_dir <> "/text.txt"
    print_file_size(input_file)

    {:ok, output_file} =
      input_file
      |> Deduplicator.deduplicate_file(@test_dir)

    chunk_repetition()

    print_file_size(output_file)
  end

  describe "Recover file" do
    test "txt" do
      input_file = @resources_dir <> "/text.txt"
      print_file_size(input_file)

      {:ok, output_file} =
        input_file
        |> Deduplicator.deduplicate_file(@test_dir)

      print_file_size(output_file)

      recovered_file = @test_dir <> "/text.txt"
      {:ok, _} = Deduplicator.recovery_file(output_file, recovered_file)
      print_file_size(recovered_file)
    end

    test "pdf" do
      input_file = @resources_dir <> "/pdf_example.pdf"
      print_file_size(input_file)

      {:ok, output_file} =
        input_file
        |> Deduplicator.deduplicate_file(@test_dir)

      print_file_size(output_file)

      recovered_file = @test_dir <> "/pdf_example.pdf"
      {:ok, _} = Deduplicator.recovery_file(output_file, recovered_file)
      print_file_size(recovered_file)
    end

    @tag timeout: 400_000
    test "jpg" do
      input_file = @resources_dir <> "/IMG_0036.jpg"
      print_file_size(input_file)

      count =
        input_file
        |> Deduplicator.Files.Reader.read()
        |> Enum.count()


      IO.puts("Chunk amount #{count}")

      {:ok, output_file} =
        input_file
        |> Deduplicator.deduplicate_file(@test_dir)

      chunk_repetition()

      print_file_size(output_file)

      recovered_file = @test_dir <> "/IMG_0036.jpg"
      {:ok, _} = Deduplicator.recovery_file(output_file, recovered_file)
      print_file_size(recovered_file)
    end
  end

  def print_file_size(filename) do
    %{size: size} = File.stat!(filename)
    IO.puts("File #{filename} size: #{size} bytes")
  end

  def chunk_repetition do
    Deduplicator.Schemas.HashLinks
    |> where([h], h.refs_num > 1)
    |> select([h], h.refs_num)
    |> Deduplicator.Repo.all()
    |> IO.inspect(label: "Chunk repetition")
  end
end
