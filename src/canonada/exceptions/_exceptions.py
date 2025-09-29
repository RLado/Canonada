class SkipItem(Exception):
    """
    Stops execution of the current item in a Pipeline without logging any errors.
    """
    
    def __init__(self, master_key: tuple = (), message: str = "Skip item"):
        super().__init__(message)
        self.message: str = message
        self.master_key: tuple = master_key

    def __str__(self) -> str:
        return f"Item '{self.master_key}' requested to skip its execution: {self.message}"

class StopPipeline(Exception):
    """
    Stops pipeline or system execution when raised.
    """
    
    def __init__(self, master_key: tuple = (), message: str = "Stop pipeline execution"):
        super().__init__(message)
        self.message: str = message
        self.master_key: tuple = master_key

    def __str__(self) -> str:
        return f"Item '{self.master_key}' requested to stop the pipeline execution: {self.message}"