from processors.puzzle_pieces_picker import PuzzlePiecesProcessor
from processors.m5hisdoc_processor import M5HisDocProcessor
from processors.casia_hwdb_processor import CasiaHWDBProcessor

def get_processor(dataset_name):
    if dataset_name == 'puzzle-pieces-picker':
        return PuzzlePiecesProcessor()
    elif dataset_name == 'm5hisdoc':
        return M5HisDocProcessor()
    elif dataset_name == 'casia-hwdb':
        return CasiaHWDBProcessor()
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")
