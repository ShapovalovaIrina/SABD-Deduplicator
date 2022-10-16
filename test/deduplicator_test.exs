defmodule DeduplicatorTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  @test_dir "test/output"

  setup_all do
    File.mkdir(@test_dir)
#    on_exit(fn -> File.rm_rf!(@test_dir) end)
    :ok
  end

  setup tags do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Deduplicator.Repo)

    unless tags[:async] do
      Ecto.Adapters.SQL.Sandbox.mode(Deduplicator.Repo, {:shared, self()})
    end

    :ok
  end

  @bytes Application.get_env(:deduplicator, :chuck_size_bytes)

  test "Read bytes from file" do
    [
      "priv/text.txt",
      "priv/pdf_example.pdf"
    ]
    |> Enum.each(fn filename ->
      %{size: initial_size} = File.stat!(filename)

      res = Deduplicator.Files.Reader.read(filename)

      res
      |> Enum.each(&byte_size(&1) == @bytes)

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
        "./priv/text.txt"
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

  test "Generate filename" do
    Deduplicator.Files.Writer.generate_filename()
    |> IO.inspect()
  end

  test "Read & save chunks" do
    input_file = "priv/text.txt"
    print_file_size(input_file)

    {:ok, output_file} =
      input_file
      |> Deduplicator.deduplicate_file("test/output")

#    Deduplicator.Schemas.HashLinks
#    |> where([h], h.refs_num > 1)
#    |> Deduplicator.Repo.all
#    |> IO.inspect(label: "Deduplicated hash")

    print_file_size(output_file)
  end

  test "Recover file" do
    input_file = "priv/text.txt"
#    input_file = "priv/pdf_example.pdf"
    print_file_size(input_file)

    {:ok, output_file} =
      input_file
      |> Deduplicator.deduplicate_file("test/output")

    print_file_size(output_file)

    recovered_file = "test/output/text.txt"
#    recovered_file = "test/output/pdf_example.pdf"
    {:ok, _} = Deduplicator.recovery_file(output_file, recovered_file)
    print_file_size(recovered_file)
  end

  def print_file_size(filename) do
    %{size: size} = File.stat!(filename)
    IO.puts("File #{filename} size: #{size} bytes")
  end
end
