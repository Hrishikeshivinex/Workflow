from typing import Any, Dict
from .base_node import BaseNode

class InputNode(BaseNode):
    async def process(self, input_data: Dict[str, Any]) -> Any:
        key = self.data.get('key')
        if key not in input_data:
            raise KeyError(f"Input key '{key}' not found in input data")
        return input_data[key] 