class InvalidDatasetException(Exception):
    def __init__(self, dataset_name: str, message: str = "") -> None:
        self.message =  f"{dataset_name} is not a valid dataset. Please recheck. {message}"        
        super().__init__(self.message)

class BadQueryException(Exception):
    def __init__(self, message: str = "") -> None:
        self.message =  f"Bad Query request. Please recheck. {message}"        
        super().__init__(self.message)