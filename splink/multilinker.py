import altair as alt



class Linker:
    def __init__(self, data):
        self.data = data

    def missingness_chart(self):
        # Implementation of the method that returns an Altair chart based on self.data
        pass

class MultiLinker:
    def __init__(self, linkers):
        self.linkers = linkers

    def display_charts(self):
        charts = [linker.missingness_chart().to_dict() for linker in self.linkers]

        # Remove the $schema and config attributes from each chart
        for chart in charts:
            chart.pop('$schema', None)
            chart.pop('config', None)

        # Reconstruct charts and concatenate
        reconstructed_charts = [alt.Chart.from_dict(chart) for chart in charts]
        combined_chart = alt.HConcatChart(hconcat=reconstructed_charts)

        return combined_chart
