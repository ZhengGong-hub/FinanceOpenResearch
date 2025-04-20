"""Project path configuration."""

from pathlib import Path

# Root directory (now points to the project root)
ROOT_DIR = Path(__file__).parent.parent.resolve()

# Common directories
COMMON_DIR = ROOT_DIR / "common"
UTILS_DIR = COMMON_DIR / "utils"
PREPROCESSING_DIR = COMMON_DIR / "preprocessing"
INTERFACE_DIR = COMMON_DIR / "interface"
R_DIR = COMMON_DIR / "r"

def get_paper_dirs(paper_id: str) -> dict:
    """Get all directories for a specific paper.
    
    Args:
        paper_id: Identifier of the paper (e.g., 'paper1_identifier')
        
    Returns:
        Dictionary containing all relevant paths for the paper
    """
    paper_dir = ROOT_DIR / "papers" / paper_id
    
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