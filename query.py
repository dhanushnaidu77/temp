def get_mongo_operator(operator):
    cases = {
        "=": "$eq",
        "!=": "$ne",
        "<": "$lt",
        ">": "$gt",
        "<=": "$lte",
        ">=": "$gte"
    }

    if operator in cases.keys():
        return cases[operator]
    return None


class Query:


    def __init__(self, operation, query_list):
        self.operation = f"${operation}"
        self.query_list = query_list
        self.query_dict = {}

        if self.query_list is not None and isinstance(self.query_list, list):
            for query in query_list:
                if isinstance(query, list):
                    feature = ""
                    if query[0] != "srvtime":
                        feature = f"value.{query[0]}"
                    else:
                        feature = query[0]

                    if len(query) == 2:
                        self.query_dict[feature] = query[1]

                    elif len(query) == 3 and get_mongo_operator(query[1]) is not None:
                        self.query_dict[feature] = {
                            get_mongo_operator(query[1]): query[2]}

                    else:
                        if query[1] <= query[2]:
                            self.query_dict[feature] = {get_mongo_operator(">="): query[1],
                                                        get_mongo_operator("<="): query[2]}

        # self.query_dict = {self.operation: [{key: value}
        #                                     for key, value in self.query_dict.items()]}

        self.query_dict = {{key: value} for key, value in self.query_dict.items()}

    def get_query(self) -> dict:
        return self.query_dict
