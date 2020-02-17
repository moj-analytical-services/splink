# from sparklink.gammas import complete_settings_dict
# from sparklink.validate import validate_settings
# from sparklink.params import Params
# from sparklink.case_statements import _check_jaro_registered
# from sparklink.blocking import block_using_rules



# class Sparklink():

#     def __init__(self, settings, df_l = None, df_r = None, df = None, spark=None):

#         _check_jaro_registered(spark)

#         settings = complete_settings_dict(settings)
#         validate_settings(settings)
#         self.settings = settings
#         self.params = Params(settings)

#     def _check_args(*args, **kwargs):
#         link_type = settings["link_type"]
#         if link_type = "dedupe_only":
#             if df

#     def generated_scored_comparisons(self, df_l, df_r, df):

#         if self.settings["link_type"] == "dedupe_only":
#             block_using_rules("dedupe_only", df_l, df_r, settings)

#         if self.settings["link_type"] == "link_only":
#             pass

#         if self.settings["link_type"] == "link_and_dedupe":
#             pass
