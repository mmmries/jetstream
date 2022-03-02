defmodule Jetstream.MixProject do
  use Mix.Project

  def project do
    [
      app: :jetstream,
      version: "0.0.1-pre2",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: [
        main: "readme",
        extras: ["README.md", "MANAGING.md"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:crypto, :logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gnat, "~> 1.1"},
      {:jason, "~> 1.1"},
      {:ex_doc, "~> 0.28", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      description: "A Jetstream client in pure Elixir.",
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/mmmries/jetstream"
      },
      maintainers: [
        "Michael Ries",
        "Benjamin Yu",
        "Marek Kaput",
        "Szymon Świerk",
        "Mariusz Morawski"
      ]
    ]
  end
end
