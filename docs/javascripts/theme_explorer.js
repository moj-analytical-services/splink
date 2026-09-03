const scriptBaseUrl = document.currentScript
  ? new URL(".", document.currentScript.src)
  : new URL("../javascripts/", window.location.href);

(function () {
  const root = document.getElementById("theme-selector-dashboard");
  if (!root) {
    // for all the other docs pages
    return;
  }

  const themeSelectEl = document.getElementById("theme-selector-input");
  const statusEl = document.getElementById("theme-selector-status");
  const chartsEl = document.getElementById("theme-selector-charts");

  if (!themeSelectEl || !chartsEl) {
    console.error("theme_explorer: required DOM elements missing");
    return;
  }

  const payloadUrl = root.getAttribute("data-payload-url");

  const vegaLibs = [
    new URL("vendor/vega.min.js", scriptBaseUrl).toString(),
    new URL("vendor/vega-lite.min.js", scriptBaseUrl).toString(),
    new URL("vendor/vega-embed.min.js", scriptBaseUrl).toString(),
  ];

  function setStatus(message) {
    if (statusEl) {
      statusEl.textContent = message;
    }
  }

  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = src;
      script.addEventListener("load", resolve);
      script.addEventListener("error", () => reject(new Error(`Failed loading ${src}`)));
      document.head.appendChild(script);
    });
  }

  async function ensureVegaEmbed() {
    if (window.vegaEmbed) {
      return;
    }

    for (const src of vegaLibs) {
      await loadScript(src);
    }

    if (!window.vegaEmbed) {
      throw new Error("vegaEmbed did not load");
    }
  }

  function createChartContainers(charts) {
    chartsEl.innerHTML = "";

    charts.forEach((chart, index) => {
      const card = document.createElement("section");
      card.className = "theme-selector-card";

      const title = document.createElement("h2");
      title.className = "theme-selector-chart-title";
      title.textContent = chart.title;

      const target = document.createElement("div");
      target.id = `theme-selector-chart-${index}`;
      target.className = "theme-selector-chart-target";

      card.appendChild(title);
      card.appendChild(target);
      chartsEl.appendChild(card);
    });
  }

  async function renderCharts(payload, theme) {
    setStatus(`Rendering ${payload.charts.length} charts for theme '${theme}'...`);

    await Promise.all(
      payload.charts.map((chart, i) =>
        window.vegaEmbed(`#theme-selector-chart-${i}`, chart.specsByTheme[theme], {
          actions: false,
          renderer: "svg",
        })
      )
    );

    setStatus(`Rendered ${payload.charts.length} charts.`);
  }

  async function init() {
    try {
      if (!payloadUrl) {
        throw new Error("No payload URL found on dashboard container");
      }

      setStatus("Loading chart runtime...");
      await ensureVegaEmbed();

      setStatus("Loading chart payload...");
      const response = await fetch(payloadUrl);
      if (!response.ok) {
        throw new Error(`Could not load payload from ${payloadUrl}`);
      }

      const payload = await response.json();
      if (!Array.isArray(payload.themes) || payload.themes.length === 0) {
        throw new Error("Payload contains no themes");
      }

      const defaultTheme = payload.themes[0];

      createChartContainers(payload.charts);

      for (const theme of payload.themes) {
        const option = document.createElement("option");
        option.value = theme;
        option.textContent = theme;
        option.selected = theme === defaultTheme;
        themeSelectEl.appendChild(option);
      }

      await renderCharts(payload, defaultTheme);

      themeSelectEl.addEventListener("change", async () => {
        await renderCharts(payload, themeSelectEl.value);
      });
    } catch (err) {
      console.error(err);
      setStatus("Error: unable to render charts. See console for details.");
    }
  }

  void init();
})();
