from .chart_definitions import bayes_factor_chart_def
from .params import Params

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False


initial_template = """
Initial probability of match (prior) = λ = {lam}
"""

col_template = """
Comparison of {col_name}.  Values are:
{col_name}_l: {value_l}
{col_name}_r: {value_r}
Comparison has {num_levels} levels
𝛾 for this comparison = {gamma_col_name} = {gamma_value}
Amongst matches, m = P(𝛾|match) = {prob_m}
Amongst non matches, u = P(𝛾|non-match) = {prob_nm}
Bayes factor = m/u = {bf}
New probability of match (updated belief): {updated_belief}
"""

end_template = """
Final probability of match = {final}
"""


def intuition_report(row_dict:dict, params:Params):
    """Generate a text summary of a row in the comparison table which explains how the match_probability was computed

    Args:
        row_dict (dict): A python dictionary representing the comparison row
        params (Params): splink params object

    Returns:
        string: The intuition report
    """

    pi = params.params["π"]
    lam = params.params["λ"]

    report = initial_template.format(lam=lam)

    gamma_keys = pi.keys() # gamma_0, gamma_1 etc.

    # Create dictionary to fill in template
    d = {}
    d["current_p"] = lam

    for gk in gamma_keys:

        col_params = pi[gk]

        d["col_name"] = col_params["column_name"]
        col_name = d["col_name"]
        if pi[gk]["custom_comparison"] == False:
            d["value_l"] = row_dict[col_name + "_l"]
            d["value_r"] = row_dict[col_name + "_r"]
        else:
            d["value_l"] = ", ".join([str(row_dict[c + "_l"]) for c in pi[gk]["custom_columns_used"] ])
            d["value_r"] = ", ".join([str(row_dict[c + "_r"]) for c in pi[gk]["custom_columns_used"] ])
        d["num_levels"] = col_params["num_levels"]

        d["gamma_col_name"] = gk
        d["gamma_value"] = row_dict[gk]

        d["prob_m"] = float(row_dict[f"prob_{gk}_match"])
        d["prob_nm"] = float(row_dict[f"prob_{gk}_non_match"])

        d["bf"] = d["prob_m"]/d["prob_nm"]

        # Update belief
        bf = d["bf"]
        current_prob = d["current_p"]

        a = bf*current_prob
        new_p = a/(a + (1-current_prob))
        d["updated_belief"] = new_p
        d["current_p"] = new_p

        col_report = col_template.format(**d)

        report += col_report

    report += end_template.format(final=new_p)

    return report

def _get_bayes_factors(row_dict, params):

    pi = params.params["π"]

    gamma_keys = pi.keys() # gamma_0, gamma_1 etc.

    bayes_factors  = []


    for gk in gamma_keys:

        col_params = pi[gk]

        column = col_params["column_name"]

        prob_m = float(row_dict[f"prob_{gk}_match"])
        prob_nm = float(row_dict[f"prob_{gk}_non_match"])

        bf = prob_m/prob_nm

        bayes_factors.append({"gamma": gk,"column": column, "bayes_factor": bf})

    return bayes_factors

def bayes_factor_chart(row_dict, params):

    bayes_factor_chart_def["data"]["values"] = _get_bayes_factors(row_dict, params)
    bayes_factor_chart_def["encoding"]["y"]["field"] = "column"
    del bayes_factor_chart_def["encoding"]["row"]
    del bayes_factor_chart_def["height"]

    if altair_installed:
        return alt.Chart.from_dict(bayes_factor_chart_def)
    else:
        return bayes_factor_chart_def

