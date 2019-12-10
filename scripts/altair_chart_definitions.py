
        chart_data = alt.Data(values=data)


        chart = alt.Chart(chart_data).mark_bar().encode(
            x='iteration:O',
            y=alt.Y('sum(probability):Q', axis=alt.Axis(title='𝛾 value')),
            color='value:N',
            row=alt.Row('column:N', sort=alt.SortField("gamma")),
            tooltip = ['probability:Q', 'iteration:O', 'column:N', 'value:N']
        ).resolve_scale(
            y='independent'
        ).properties(height=100)


        c0 = chart.transform_filter(
            (datum.match == 0)
        ).properties(title="Non match")

        c1 = chart.transform_filter(
            (datum.match == 1)
        ).properties(title="Match")

        facetted_chart = c0 | c1

        fc = facetted_chart.configure_title(
            anchor='middle'
        ).properties(
            title='Probability distribution of comparison vector values by iteration number'
        )


                data = params.convert_params_dict_to_data(params.params)

        chart_data = alt.Data(values=data)
        chart = alt.Chart(chart_data).mark_bar().encode(
            x='probability:Q',
            y=alt.Y('value:N', axis=alt.Axis(title='𝛾 value')),
            color='match:N',
            row=alt.Row('column:N', sort=alt.SortField("gamma")),
            tooltip=["column:N", alt.Tooltip('probability:Q', format='.4f'), "value:O"]
        ).resolve_scale(
            y='independent'
        ).properties(width=150)


        c0 = chart.transform_filter(
            (datum.match == 0)
        )

        c1 = chart.transform_filter(
            (datum.match == 1)
        )

        facetted_chart = c0 | c1

        fc = facetted_chart.configure_title(
            anchor='middle'
        ).properties(
            title='Probability distribution of comparison vector values, m=0 and m=1'
        )

        fc
        d = fc.to_dict()
        d["data"]["values"] = None
        d