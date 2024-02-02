class InvalidDatasetException(Exception):
    def __init__(self, dataset_name: str, message: str = "") -> None:
        self.message =  f"{dataset_name} is not a valid dataset. Please recheck. {message}"        
        super().__init__(self.message)
