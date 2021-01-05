#         chart_data = alt.Data(values=data)


#         chart = alt.Chart(chart_data).mark_bar().encode(
#             x='iteration:O',
#             y=alt.Y('sum(probability):Q', axis=alt.Axis(title='𝛾 value')),
#             color='value:N',
#             row=alt.Row('column:N', sort=alt.SortField("gamma")),
#             tooltip = ['probability:Q', 'iteration:O', 'column:N', 'value:N']
#         ).resolve_scale(
#             y='independent'
#         ).properties(height=100)


#         c0 = chart.transform_filter(
#             (datum.match == 0)
#         ).properties(title="Non match")

#         c1 = chart.transform_filter(
#             (datum.match == 1)
#         ).properties(title="Match")

#         facetted_chart = c0 | c1

#         fc = facetted_chart.configure_title(
#             anchor='middle'
#         ).properties(
#             title='Probability distribution of comparison vector values by iteration number'
#         )


#                 data = params.convert_params_dict_to_data(model.current_settings_obj)

#         chart_data = alt.Data(values=data)
#         chart = alt.Chart(chart_data).mark_bar().encode(
#             x='probability:Q',
#             y=alt.Y('value:N', axis=alt.Axis(title='𝛾 value')),
#             color='match:N',
#             row=alt.Row('column:N', sort=alt.SortField("gamma")),
#             tooltip=["column:N", alt.Tooltip('probability:Q', format='.4f'), "value:O"]
#         ).resolve_scale(
#             y='independent'
#         ).properties(width=150)


#         c0 = chart.transform_filter(
#             (datum.match == 0)
#         )

#         c1 = chart.transform_filter(
#             (datum.match == 1)
#         )

#         facetted_chart = c0 | c1

#         fc = facetted_chart.configure_title(
#             anchor='middle'
#         ).properties(
#             title='Probability distribution of comparison vector values, m=0 and m=1'
#         )

#         fc
#         d = fc.to_dict()
#         d["data"]["values"] = None
#         d


# ---
# chart_data = alt.Data(values=adjustment_factors)
# chart = alt.Chart(chart_data).mark_bar().encode(
#         x=alt.X('normalised:Q', scale=alt.Scale(domain=[-0.5,0.5])),
#         y=alt.Y('col_name:N', sort=alt.SortField("gamma")),
#         color = alt.Color('normalised:Q', scale=alt.Scale(domain=[-0.5, -0.4, 0, 0.4, 0.5], range=['red', 'orange', 'green', 'orange', 'red'])),
#         tooltip = ['field:N', 'normalised:Q'])
# chart


# chart_data = alt.Data(values=data)
# chart = alt.Chart(chart_data).mark_bar().encode(
#         x=alt.X('normalised_adjustment:Q', scale=alt.Scale(domain=[-0.5,0.5]), axis=alt.Axis(title='Influence on match probabiity.')),
#         y=alt.Y('level:N'),
#         row=alt.Row('col_name:N',  sort=alt.SortField("gamma")),
#         color = alt.Color('normalised_adjustment:Q', scale=alt.Scale(domain=[-0.5, -0.4, 0, 0.4, 0.5], range=['red', 'orange', 'green', 'orange', 'red'])),
#         tooltip = ['col_name:N', 'normalised_adjustment:Q']
#         ).resolve_scale(
#             y='independent'
#         ).properties(height=50, title="Influence of comparison vector values on match probability"
# ).configure_title(
#             anchor='middle'
#         )
# chart


# def get_chart(percentile_data, top_n_data, col_name):

#     data = alt.Data(values=percentile_data)

#     c_left = alt.Chart(data).mark_line().encode(
#         x=alt.X('percentile:Q', sort='descending', title='Percentile'),
#         y=alt.Y('token_count:Q', title='Count of values'),
#     ).properties(
#         title=f'Distribution of counts of values in column {col_name}',

#     )

#     data = alt.Data(values=top_n_data)

#     c_right = alt.Chart(data).mark_bar().encode(
#         x=alt.X(f'value:N', sort='-y', title=None),
#         y=alt.Y('count:Q', title='Value count')
#     ).properties(
#     title='Top 20 values by value count',

# )

#     return alt.hconcat(c_left, c_right)

# estimate comaprison
# import altair as alt
# from altair import datum
# c = alt.Chart(df).mark_point(
#     size=100,
#     opacity=1,
#     filled=True
# ).encode(
#     x='u_probability:Q',
#     y=alt.Y('level_name:N',axis=alt.Axis(grid=True, title=None)),
#     color=alt.Color('estimate_name:N'),
#     row=alt.Row('column_name:N', title=None)
# ).resolve_scale(y='independent'
# ).properties(title= "Non-matches"
# ).interactive()


# c1 = c.transform_filter(
#     datum.gamma_index != 1000000
# )

# c2 = c.encode(x='m_probability:Q' ).transform_filter(
#     datum.gamma_index != 1000001
# ).properties(title= "Matches")

# chart = (c1 | c2).configure_title(anchor='middle')
# chart = chart.properties(title='Comparison of parameter estimates between jobs')

# chart