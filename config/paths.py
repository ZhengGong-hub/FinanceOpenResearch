"""Project path configuration."""

from pathlib import Path
from typing import Dict, Any


class ProjectPaths:
    """Class to manage all project paths."""
    
    def __init__(self):
        """Initialize project paths."""
        self.root_dir = Path(__file__).parent.parent.resolve()
        
        # Common directories
        self.common_dir = self.root_dir / "common"
        self.utils_dir = self.common_dir / "utils"
        self.preprocessing_dir = self.common_dir / "preprocessing"
        self.interface_dir = self.common_dir / "interface"
        self.r_dir = self.common_dir / "r"
        
        # Other root directories
        self.config_dir = self.root_dir / "config"
        self.docs_dir = self.root_dir / "docs"
        self.scripts_dir = self.root_dir / "scripts"
        self.papers_dir = self.root_dir / "papers"
    
    def get_paper_paths(self, paper_id: str) -> Dict[str, Any]:
        """Get all paths for a specific paper.
        
        Args:
            paper_id: Identifier of the paper (e.g., 'paper1_identifier')
            
        Returns:
            Dictionary containing all relevant paths for the paper
        """
        paper_dir = self.papers_dir / paper_id
        
        return {
            "root": paper_dir,
            "data": {
                "raw": paper_dir / "data" / "raw",
                "processed": paper_dir / "data" / "processed",
            },
            "src": paper_dir / "src",
            "scripts": paper_dir / "scripts",
            "notebooks": paper_dir / "notebooks",
            "tests": paper_dir / "tests",
            "output": {
                "root": paper_dir / "output",
                "figures": paper_dir / "output" / "figures",
                "tables": paper_dir / "output" / "tables",
                "logs": paper_dir / "output" / "logs",
            },
            "paper": {
                "root": paper_dir / "paper",
                "figures": paper_dir / "paper" / "figures",
                "sections": paper_dir / "paper" / "sections",
            },
        }
    
    def ensure_directories(self) -> None:
        """Create all necessary directories if they don't exist."""
        # Create common directories
        self.utils_dir.mkdir(parents=True, exist_ok=True)
        self.preprocessing_dir.mkdir(parents=True, exist_ok=True)
        self.interface_dir.mkdir(parents=True, exist_ok=True)
        self.r_dir.mkdir(parents=True, exist_ok=True)
        
        # Create other root directories
        self.docs_dir.mkdir(parents=True, exist_ok=True)
        self.scripts_dir.mkdir(parents=True, exist_ok=True)
        self.papers_dir.mkdir(parents=True, exist_ok=True)
    
    def create_paper_structure(self, paper_id: str) -> None:
        """Create the complete directory structure for a new paper.
        
        Args:
            paper_id: Identifier of the paper
        """
        paths = self.get_paper_paths(paper_id)
        
        # Create all directories
        for path_dict in [paths["data"], paths["output"], paths["paper"]]:
            for path in path_dict.values():
                if isinstance(path, Path):
                    path.mkdir(parents=True, exist_ok=True)
        
        # Create other paper directories
        paths["src"].mkdir(parents=True, exist_ok=True)
        paths["scripts"].mkdir(parents=True, exist_ok=True)
        paths["notebooks"].mkdir(parents=True, exist_ok=True)
        paths["tests"].mkdir(parents=True, exist_ok=True)
