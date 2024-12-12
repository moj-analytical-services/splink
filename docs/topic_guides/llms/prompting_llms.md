# Using LLMs such as ChatGPT to help you write Splink code

We provide two files that summarise the Splink docs to help you use Splink with LLMs:

[Concise Splink Documentation Summary](https://gist.githubusercontent.com/RobinL/2257a0253d80ab8e651a5ec08bc3d00a/raw/dc656a3c698848473050ba6fd56c6595c7837b52/llm_context_short.txt) - around 25,000 tokens

[Comprehensive Splink Documentation Summary](https://gist.githubusercontent.com/RobinL/2257a0253d80ab8e651a5ec08bc3d00a/raw/dc656a3c698848473050ba6fd56c6595c7837b52/llm_context_long.txt) - around 90,000 tokens

At present, only the concise one is short enough for the ChatGPT GUI. The longer one fits in the Claude long context (200k tokens) model.

### Recommended use

To use these files, we suggest copying and pasing their contents into the LLM, and then appending your question.

Alternative, if you're an OpenAI user, you can use the [Splink 4 GPT](https://chatgpt.com/g/g-szbSxsv0u-splink-4-helper), which includes these context files in their knowledge

For instance, you may paste their contents and then ask:

```
I have a dataset I want to dedupe using duckdb

It has columns full_name, dob, occupation, zip

Do some preparatory data cleaning and run a splink script to train a model and dedupe
```

Which results in (ChatGPT `o1` model)

<details>
<summary>Click to see example response</summary>

```python
import duckdb
from splink import Linker, SettingsCreator, DuckDBAPI, block_on
import splink.comparison_library as cl

# Load and clean data in DuckDB
con = duckdb.connect()
con.execute("""
    CREATE TABLE raw_data AS SELECT * FROM read_csv('your_data.csv', HEADER=TRUE)
""")

# Example cleaning steps: lowercasing fields, removing punctuation from full_name, standardizing dob format
con.execute("""
    CREATE TABLE cleaned AS
    SELECT
        regexp_replace(lower(full_name), '[^a-z ]', '', 'g') as full_name,
        regexp_replace(dob, '[^0-9-]', '', 'g') as dob,
        lower(occupation) as occupation,
        regexp_replace(lower(zip), '[^a-z0-9 ]', '', 'g') as zip
    FROM raw_data
""")

df = con.table("cleaned")

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("full_name"),
        block_on("dob"),
        block_on("zip"),
    ],
    comparisons=[
        cl.NameComparison("full_name"),
        cl.DateOfBirthComparison("dob", input_is_string=True),
        cl.ExactMatch("occupation").configure(term_frequency_adjustments=True),
        cl.LevenshteinAtThresholds("zip", 2),
    ],
    retain_intermediate_calculation_columns=True,
)

db_api = DuckDBAPI()
linker = Linker(df, settings, db_api=db_api)

deterministic_rules = [
    "l.full_name = r.full_name and l.dob = r.dob",
    "l.zip = r.zip and levenshtein(l.full_name, r.full_name) < 2"
]

linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
linker.training.estimate_parameters_using_expectation_maximisation(block_on("full_name"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

predictions = linker.inference.predict(threshold_match_probability=0.9)
clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(predictions, threshold_match_probability=0.95)
```
</details>

!!! warning

    Whilst these prompts can be helpful to give you some initial code, there's **no guarantees** it will be correct.

    However, in many cases it can provide a useful starting point.

You can also ask more theoretical questions such as:

```
Explain briefly how we can calculate the prior in Splink.  Given the number of matches is unknown, how can we calculate the probability two random records match?  Give a code example.
```

<details>
<summary>Click to see example response</summary>

```
Splink typically uses a set of deterministic rules that identify a subset of highly probable matches. By assuming these rules have a known “recall” (i.e. they capture a certain proportion of all true matches), Splink infers the total number of matches and, from that, calculates the probability that two random records match. In other words, you pick deterministic rules, count how many pairs they produce, estimate what fraction of all true matches they represent, and then scale up that count to the entire dataset, giving you an approximate prior probability.
```
</details>


### How these prompts are generated

These files are generated with the following scripts, which you may wish to modify for your purposes:

[Create LLM Prompt Long Script](https://github.com/moj-analytical-services/splink/blob/master/scripts/create_llm_prompt_long.py)

[Create LLM Prompt Short Script](https://github.com/moj-analytical-services/splink/blob/master/scripts/create_llm_prompt_short.py)

### Issues and Discussions

I have also created a knowledgebase file by scraping all Github issues and discussions from the main Splink repo.

I've found Gemini Pro 1.5 (2 million context length) is able to process these files effectively.

[Knowledgebase prompt - 1.6 million tokens](https://gist.github.com/RobinL/f241917bc4246911f12aed2b372d7f43#file-splink_knowledge_base-txt)

[Create knowledgebase script](https://gist.github.com/RobinL/f241917bc4246911f12aed2b372d7f43#file-creation_script-py)