defmodule Jetstream.API.Util do
  @moduledoc false

  def request(conn, topic, payload) do
    with {:ok, %{body: body}} <- Gnat.request(conn, topic, payload),
         {:ok, decoded} <- Jason.decode(body) do
      case decoded do
        %{"error" => err} ->
          {:error, err}

        other ->
          {:ok, other}
      end
    end
  end

  def to_datetime(nil), do: nil

  def to_datetime(str) do
    {:ok, datetime, _} = DateTime.from_iso8601(str)
    datetime
  end

  def to_sym(nil), do: nil

  def to_sym(str) when is_binary(str) do
    String.to_existing_atom(str)
  end

  def put_if_exist(target_map, target_key, source_map, source_key) do
    case Map.fetch(source_map, source_key) do
      {:ok, value} -> Map.put(target_map, target_key, value)
      _ -> target_map
    end
  end
end
