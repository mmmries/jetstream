defmodule OffBroadway.Jetstream.AcknowledgerTest do
  use Jetstream.ConnCase

  alias OffBroadway.Jetstream.Acknowledger

  describe "init/1" do
    test "returns config with default actions" do
      assert {:ok, ref} = Acknowledger.init(connection_name: :gnat)

      assert Acknowledger.get_config(ref) ==
               %Acknowledger{
                 connection_name: :gnat,
                 on_failure: :nack,
                 on_success: :ack
               }
    end

    test "with valid options, returns config with custom actions" do
      assert {:ok, ref} =
               Acknowledger.init(connection_name: :gnat, on_success: :term, on_failure: :ack)

      assert Acknowledger.get_config(ref) ==
               %Acknowledger{
                 connection_name: :gnat,
                 on_failure: :ack,
                 on_success: :term
               }
    end
  end

  describe "configure/3" do
    test "raise on unsupported configure option" do
      assert_raise(ArgumentError, "unsupported configure option :on_other", fn ->
        Acknowledger.configure(:ack_ref, %{}, on_other: :ack)
      end)
    end

    test "raise on unsupported on_success value" do
      error_msg = "expected :on_success to be a valid acknowledgement option, got: :unknown"

      assert_raise(ArgumentError, error_msg, fn ->
        Acknowledger.configure(:ack_ref, %{}, on_success: :unknown)
      end)
    end

    test "raise on unsupported on_failure value" do
      error_msg = "expected :on_failure to be a valid acknowledgement option, got: :unknown"

      assert_raise(ArgumentError, error_msg, fn ->
        Acknowledger.configure(:ack_ref, %{}, on_failure: :unknown)
      end)
    end

    test "set on_success correctly" do
      ack_data = %{reply_to: "sample_topic"}
      expected = %{reply_to: "sample_topic", on_success: :term}

      assert {:ok, expected} == Acknowledger.configure(:ack_ref, ack_data, on_success: :term)
    end

    test "set on_success with ignore" do
      ack_data = %{reply_to: "sample_topic"}
      expected = %{reply_to: "sample_topic", on_success: :term}

      assert {:ok, expected} ==
               Acknowledger.configure(:ack_ref, ack_data, on_success: :term)
    end
  end

  describe "ack/3" do
    @describetag with_gnat: :gnat

    setup tags do
      ack_topic = "ack_topic"

      {:ok, _sid} = Gnat.sub(:gnat, self(), ack_topic)

      opts = [connection_name: :gnat]

      opts =
        if on_success = tags[:on_success] do
          Keyword.put(opts, :on_success, on_success)
        else
          opts
        end

      opts =
        if on_failure = tags[:on_failure] do
          Keyword.put(opts, :on_failure, on_failure)
        else
          opts
        end

      {:ok, ack_ref} = Acknowledger.init(opts)

      %{ack_ref: ack_ref, ack_topic: ack_topic}
    end

    test "successful messages are acknowledged with default settings", %{
      ack_ref: ack_ref,
      ack_topic: ack_topic
    } do
      {successful, failed} =
        example_messages(10, ack_ref, ack_topic)
        |> Enum.split(5)

      :ok = Acknowledger.ack(ack_ref, successful, failed)

      for _ <- 1..5, do: assert_receive({:msg, %{body: "", topic: ^ack_topic}})

      for _ <- 1..5, do: assert_receive({:msg, %{body: "-NAK", topic: ^ack_topic}})
    end

    @tag on_success: :term
    test "supports custom on_success setting", %{ack_ref: ack_ref, ack_topic: ack_topic} do
      {successful, failed} =
        example_messages(10, ack_ref, ack_topic)
        |> Enum.split(5)

      :ok = Acknowledger.ack(ack_ref, successful, failed)

      for _ <- 1..5, do: assert_receive({:msg, %{body: "+TERM", topic: ^ack_topic}})

      for _ <- 1..5, do: assert_receive({:msg, %{body: "-NAK", topic: ^ack_topic}})
    end

    @tag on_failure: :term
    test "supports custom on_failure setting", %{ack_ref: ack_ref, ack_topic: ack_topic} do
      {successful, failed} =
        example_messages(10, ack_ref, ack_topic)
        |> Enum.split(5)

      :ok = Acknowledger.ack(ack_ref, successful, failed)

      for _ <- 1..5, do: assert_receive({:msg, %{body: "", topic: ^ack_topic}})

      for _ <- 1..5, do: assert_receive({:msg, %{body: "+TERM", topic: ^ack_topic}})
    end
  end

  defp example_messages(quantity, ack_ref, ack_topic) do
    for i <- 1..quantity do
      i
      |> to_string()
      |> example_message(ack_ref, ack_topic)
    end
  end

  defp example_message(data, ack_ref, ack_topic) do
    acknowledger = Acknowledger.builder(ack_ref).(ack_topic)

    %Broadway.Message{
      acknowledger: acknowledger,
      data: data
    }
  end
end
