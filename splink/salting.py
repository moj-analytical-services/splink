def _add_salt_to_blocking_rules(rules, salting=1):

    # if not self._salting > 1:
    #     return rules

    # Output format for blocking rules with salt:
    # {
    #   "original_rule and __splink_salt = 1":
    #   [
    #    "__splink_salt = 1",
    #    [previous_rule_1, previous_rule_2]
    #   ]
    # }

    # This gives us a simple format to use when building our
    # blocking rules in blocking.py. We can evaluate the previous_rule
    # list to see if len() > 1 and then grab rules + salt to create the
    # not_previous_rules_statement SQL.

    if not isinstance(rules, list):
        rules = [rules]

    blocking_rules = {}
    previous_rules = []
    add_salt = True if salting > 1 else False
    # if we are applying salt, then run create more blocking rules
    # with the salt applied in the loop below.
    iterations = salting + 1 if salting else 2
    for rule in rules:

        for n in range(1, iterations):
            if add_salt:
                salt = f"l.__splink_salt = {n}"
                salting_rule = f"{rule} and {salt}"
            else:
                salting_rule = rule
            out_salt = f"not {salt}" if add_salt else None
            blocking_rules[salting_rule] = [out_salt, previous_rules.copy()]

        previous_rules.append(rule)

    return blocking_rules
