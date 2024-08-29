import os
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.argv = ["canonada", "new"]
import canonada.cli as cli


class TestProjectCLI(unittest.TestCase):
    """
    Test basic CLI project interactions
    """

    def test_project_creation(self):
        """
        Emulate argv input for project creation
        """
        # Change sys.argv to emulate input
        sys.argv = ["canonada", "new", "test_project"]

        # Make a new directory for the test project
        os.chdir(os.path.dirname(__file__))
        os.makedirs("test_project_creation")
        os.chdir("test_project_creation")

        # Emulate argv input for project creation
        cli.main()

        # Check if the project was created successfully
        expected_dirs = [
            "config",
            "data",
            "datahandlers",
            "notebooks",
            "pipelines",
            "pipelines/nodes",
            "systems",
            "tests",
        ]
        expected_files = [
            ".gitignore",
            "canonada.toml",
            "config/catalog.toml",
            "config/parameters.toml",
            "config/credentials.toml",
            "datahandlers/__init__.py",
            "pipelines/__init__.py",
            "pipelines/nodes/__init__.py",
            "systems/__init__.py",
        ]
        for directory in expected_dirs:
            self.assertTrue(os.path.isdir(directory)) 
        for file in expected_files:
            self.assertTrue(os.path.isfile(file))
        
        # Clean up
        os.chdir("..")
        os.system("rm -rf test_project_creation")
    
    def test_empty_project_commands(self):
        """
        Test the catalog, registry, and version commands on an empty project
        """
        # Change sys.argv to emulate input
        sys.argv = ["canonada", "new", "test_project"]

        # Make a new directory for the test project
        os.chdir(os.path.dirname(__file__))
        os.makedirs("test_empty_project_commands")
        os.chdir("test_empty_project_commands")

        # Emulate argv input for project creation
        cli.main()

        # Emulate argv input for catalog command
        sys.argv = ["canonada", "catalog", "list"]
        cli.main() # Should not raise an error
        sys.argv = ["canonada", "catalog", "params"]
        cli.main() # Should not raise an error

        # Emulate argv input for registry command
        sys.argv = ["canonada", "registry", "pipelines"]
        cli.main() # Should not raise an error
        sys.argv = ["canonada", "registry", "systems"]
        cli.main() # Should not raise an error

        # Emulate argv input for version command
        sys.argv = ["canonada", "version"]
        cli.main() # Should not raise an error

        # Clean up
        os.chdir("..")
        os.system("rm -rf test_empty_project_commands")


if __name__ == '__main__':
    unittest.main()
