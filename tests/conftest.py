"""tests가 어디서 실행되든 extract 패키지를 찾도록 저장소 루트를 path에 얹는다."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
