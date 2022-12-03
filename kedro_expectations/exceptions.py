class KedroGreatException(Exception):
    pass

class KedroExpectationsNotInitialized(KedroGreatException):
    print("Please run \'kedro expectations init\' before using the plugin")

class UnsupportedDataSet(KedroGreatException):
    pass


class SuiteValidationFailure(KedroGreatException):
    pass