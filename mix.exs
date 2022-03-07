defmodule Jetstream.MixProject do
  use Mix.Project

  @version "0.0.1-pre2"
  @github "https://github.com/mmmries/jetstream"

  def project do
    [
      app: :jetstream,
      version: @version,
      elixir: "~> 1.10",
      source_url: @github,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs()
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
      {:connection, "~> 1.1"},
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
        "GitHub" => @github
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

  defp docs do
    [
      main: "overview",
      source_ref: "v#{@version}",
      extras: [
        "docs/introduction/overview.md",
        "docs/introduction/getting_started.md",
        "docs/guides/managing.md",
        "docs/guides/push_based_consumer.md"
      ],
      groups_for_extras: [
        Introduction: ~r/docs\/introduction\/[^\/]+\.md/,
        Guides: ~r/docs\/guides\/[^\/]+\.md/
      ]
    ]
  end
end
