class DataSource:
    def __init__(self):
        pass

    def fetch(self):
        raise NotImplementedError("Subclasses must implement fetch()")
