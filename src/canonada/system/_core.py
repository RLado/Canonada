import io

from ..logger import logger as log
from ..pipeline import Pipeline


class System():
    """
    System data structure for canonada construction.
    """
    registry = []

    @classmethod
    def ls(cls):
        """
        List all available systems
        """
        return cls.registry

    def __init__(self, name:str, pipelines:list[Pipeline], description:str=""):
        self.name:str = name
        self.description:str = description
        self.pipeline:list[Pipeline] = pipelines

        # Check that the system name is unique and not empty
        if self.name == "":
            raise ValueError("System name cannot be empty")
        if self.name in [sys.name for sys in System.registry]:
            raise ValueError(f"System name '{self.name}' is not unique")

        # Register the system
        System.registry.append(self)
    
    def __repr__(self) -> str:
        """
        Show all pipelines in the system
        """

        repr_buffer = io.StringIO()

        repr_buffer.write(f"------ System: {self.name} ------\n")
        if self.description != "":
            repr_buffer.write(f"Description: {self.description}\n")
            repr_buffer.write("---------------------------------\n")
        repr_buffer.write("\n")

        for pipe in self.pipeline:
            repr_buffer.write(f"{pipe}\n")

        return repr_buffer.getvalue()

    def __len__(self) -> int:
        """
        Return the number of pipelines in the system
        """
        return len(self.pipeline)

    def __call__(self):
        """
        Run the system
        """
        self.run()
    
    def run(self):
        """
        Run the system pipelines sequentially
        """
        log.info(f"Running pipeline system: '{self.name}'")
        for pipeline in self.pipeline:
            pipeline()


    
    