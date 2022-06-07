defmodule Jetstream.API.StreamTest do
  use Jetstream.ConnCase
  alias Jetstream.API.Stream

  @moduletag with_gnat: :gnat

  test "listing and creating, and deleting streams" do
    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert streams == nil || !("LIST_TEST" in streams)

    stream = %Stream{name: "LIST_TEST", subjects: ["STREAM_TEST"]}
    {:ok, response} = Stream.create(:gnat, stream)
    assert response.config == stream

    assert response.state == %{
             bytes: 0,
             consumer_count: 0,
             first_seq: 0,
             first_ts: ~U[0001-01-01 00:00:00Z],
             last_seq: 0,
             last_ts: ~U[0001-01-01 00:00:00Z],
             messages: 0,
             deleted: nil,
             lost: nil,
             num_deleted: nil,
             num_subjects: nil,
             subjects: nil
           }

    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert "LIST_TEST" in streams

    assert :ok = Stream.delete(:gnat, "LIST_TEST")
    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert streams == nil || !("LIST_TEST" in streams)
  end

  test "updating a stream" do
    stream = %Stream{name: "UPDATE_TEST", subjects: ["STREAM_TEST"]}
    assert {:ok, _response} = Stream.create(:gnat, stream)
    updated_stream = %Stream{name: "UPDATE_TEST", subjects: ["STREAM_TEST", "NEW_SUBJECT"]}
    assert {:ok, response} = Stream.update(:gnat, updated_stream)
    assert response.config.subjects == ["STREAM_TEST", "NEW_SUBJECT"]
    assert :ok = Stream.delete(:gnat, "UPDATE_TEST")
  end

  test "failed deletes" do
    assert {:error, %{"code" => 404, "description" => "stream not found"}} =
             Stream.delete(:gnat, "NaN")
  end

  describe "info/3" do
    test "getting stream info" do
      stream = %Stream{name: "INFO_TEST", subjects: ["INFO_TEST.*"]}
      assert {:ok, _response} = Stream.create(:gnat, stream)

      assert {:ok, response} = Stream.info(:gnat, "INFO_TEST")
      assert response.config == stream

      assert response.state == %{
               bytes: 0,
               consumer_count: 0,
               first_seq: 0,
               first_ts: ~U[0001-01-01 00:00:00Z],
               last_seq: 0,
               last_ts: ~U[0001-01-01 00:00:00Z],
               messages: 0,
               deleted: nil,
               lost: nil,
               num_deleted: nil,
               num_subjects: nil,
               subjects: nil
             }
    end

    test "getting stream info with subjects_filter" do
      stream = %Stream{name: "INFO_TEST_FILTER", subjects: ["INFO_TEST_FILTER.*"]}
      assert {:ok, _response} = Stream.create(:gnat, stream)

      assert :ok = Gnat.pub(:gnat, "INFO_TEST_FILTER.foo", "foo")
      assert :ok = Gnat.pub(:gnat, "INFO_TEST_FILTER.bar", "bar")

      assert {:ok, response} =
               Stream.info(:gnat, "INFO_TEST_FILTER", subjects_filter: "INFO_TEST_FILTER.foo")

      assert response.config == stream
      assert response.state.subjects["INFO_TEST_FILTER.foo"] == 1
    end
  end

  test "creating a stream with non-standard settings" do
    stream = %Stream{
      name: "ARGS_TEST",
      subjects: ["ARGS_TEST.*"],
      retention: :workqueue,
      duplicate_window: 100_000_000,
      storage: :memory
    }

    assert {:ok, %{config: result}} = Stream.create(:gnat, stream)
    assert result.name == "ARGS_TEST"
    assert result.duplicate_window == 100_000_000
    assert result.retention == :workqueue
    assert result.storage == :memory
  end

  test "validating stream names" do
    assert {:error, reason} =
             Stream.create(:gnat, %Stream{name: "test.periods", subjects: ["foo"]})

    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} =
             Stream.create(:gnat, %Stream{name: "test>greater", subjects: ["foo"]})

    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} = Stream.create(:gnat, %Stream{name: "test_star*", subjects: ["foo"]})
    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} =
             Stream.create(:gnat, %Stream{name: "test-space ", subjects: ["foo"]})

    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} = Stream.create(:gnat, %Stream{name: "\ttest-tab", subjects: ["foo"]})
    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"
  end

  describe "get_message/3" do
    test "error if both seq and last_by_subj are used" do
      assert {:error, reason} = Stream.get_message(:gnat, "foo", %{seq: 1, last_by_subj: "bar"})
      assert reason == "To get a message you must use only one of `seq` or `last_by_subj`"
    end

    test "decodes message data" do
      stream = %Stream{name: "GET_MESSAGE_TEST", subjects: ["GET_MESSAGE_TEST.foo"]}
      assert {:ok, _response} = Stream.create(:gnat, stream)
      assert :ok = Gnat.pub(:gnat, "GET_MESSAGE_TEST.foo", "hi there")

      assert {:ok, response} =
               Stream.get_message(:gnat, "GET_MESSAGE_TEST", %{
                 last_by_subj: "GET_MESSAGE_TEST.foo"
               })

      %{
        data: "hi there",
        hdrs: nil,
        subject: "GET_MESSAGE_TEST.foo",
        time: %DateTime{}
      } = response

      assert is_number(response.seq)

      assert :ok = Stream.delete(:gnat, "GET_MESSAGE_TEST")
    end

    test "decodes message data with headers" do
      stream = %Stream{
        name: "GET_MESSAGE_TEST_WITH_HEADERS",
        subjects: ["GET_MESSAGE_TEST_WITH_HEADERS.bar"]
      }

      assert {:ok, _response} = Stream.create(:gnat, stream)

      assert :ok =
               Gnat.pub(:gnat, "GET_MESSAGE_TEST_WITH_HEADERS.bar", "hi there",
                 headers: [{"foo", "bar"}]
               )

      assert {:ok, response} =
               Stream.get_message(:gnat, "GET_MESSAGE_TEST_WITH_HEADERS", %{
                 last_by_subj: "GET_MESSAGE_TEST_WITH_HEADERS.bar"
               })

      assert response.hdrs =~ "foo: bar"

      assert :ok = Stream.delete(:gnat, "GET_MESSAGE_TEST_WITH_HEADERS")
    end
  end

  describe "purge/2" do
    test "clears the stream" do
      stream = %Stream{name: "PURGE_TEST", subjects: ["PURGE_TEST.foo"]}
      assert {:ok, _response} = Stream.create(:gnat, stream)
      assert :ok = Gnat.pub(:gnat, "PURGE_TEST.foo", "hi there")

      assert :ok = Stream.purge(:gnat, "PURGE_TEST")

      assert {:error, %{"description" => description}} =
               Stream.get_message(:gnat, "PURGE_TEST", %{
                 last_by_subj: "PURGE_TEST.foo"
               })

      assert description in ["no message found", "stream store EOF"]

      assert :ok = Stream.delete(:gnat, "PURGE_TEST")
    end
  end

  describe "get_all_messages/2" do
    setup do
      on_exit(fn ->
        nil
      end)

      :ok
    end

    test "gets multiple messages" do
      stream = %Stream{
        name: "GET_MULTIPLE_MESSAGES",
        subjects: ["GET_MULTIPLE_MESSAGES.*"]
      }

      assert {:ok, _response} = Stream.create(:gnat, stream)

      assert :ok = Gnat.pub(:gnat, "GET_MULTIPLE_MESSAGES.bar", "bar message")
      assert :ok = Gnat.pub(:gnat, "GET_MULTIPLE_MESSAGES.foo", "foo message")

      assert {:ok, ["bar message", "foo message"]} ==
               Stream.get_all_messages(:gnat, "GET_MULTIPLE_MESSAGES")

      Stream.delete(:gnat, "GET_MULTIPLE_MESSAGES")
    end

    test "when no messages" do
      stream = %Stream{
        name: "GET_MULTIPLE_MESSAGES",
        subjects: ["GET_MULTIPLE_MESSAGES.*"]
      }

      assert {:ok, _response} = Stream.create(:gnat, stream)

      assert {:ok, []} ==
               Stream.get_all_messages(:gnat, "GET_MULTIPLE_MESSAGES")

      Stream.delete(:gnat, "GET_MULTIPLE_MESSAGES")
    end

    test "filter subject" do
      stream = %Stream{
        name: "GET_MULTIPLE_MESSAGES",
        subjects: ["GET_MULTIPLE_MESSAGES.*"]
      }

      assert {:ok, _response} = Stream.create(:gnat, stream)

      assert :ok = Gnat.pub(:gnat, "GET_MULTIPLE_MESSAGES.bar", "bar message")
      assert :ok = Gnat.pub(:gnat, "GET_MULTIPLE_MESSAGES.foo", "foo message")

      assert {:ok, ["foo message"]} ==
               Stream.get_all_messages(:gnat, "GET_MULTIPLE_MESSAGES",
                 filter_subject: "GET_MULTIPLE_MESSAGES.foo"
               )

      Stream.delete(:gnat, "GET_MULTIPLE_MESSAGES")
    end

    test "filter subject that doesn't match anything" do
      stream = %Stream{
        name: "GET_MULTIPLE_MESSAGES",
        subjects: ["GET_MULTIPLE_MESSAGES.*"]
      }

      assert {:ok, _response} = Stream.create(:gnat, stream)

      assert :ok = Gnat.pub(:gnat, "GET_MULTIPLE_MESSAGES.bar", "bar message")
      assert :ok = Gnat.pub(:gnat, "GET_MULTIPLE_MESSAGES.foo", "foo message")

      assert {:ok, []} ==
               Stream.get_all_messages(:gnat, "GET_MULTIPLE_MESSAGES",
                 filter_subject: "GET_MULTIPLE_MESSAGES.baz"
               )

      Stream.delete(:gnat, "GET_MULTIPLE_MESSAGES")
    end

    test "error if no stream" do
      assert {:error, %{"code" => 404, "description" => "stream not found", "err_code" => 10059}} ==
               Stream.get_all_messages(:gnat, "GET_MULTIPLE_MESSAGES")
    end
  end
end
