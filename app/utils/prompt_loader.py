import yaml
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class PromptLoader:
    """Utility class for loading Claude prompts from YAML configuration"""
    
    def __init__(self, prompts_file: str = "prompts.yml"):
        self.prompts_file = Path(prompts_file)
        self._prompts = {}
        self.load_prompts()
    
    def load_prompts(self) -> None:
        """Load prompts from YAML file"""
        try:
            if not self.prompts_file.exists():
                logger.error(f"Prompts file not found: {self.prompts_file}")
                self._prompts = self._get_default_prompts()
                return
            
            with open(self.prompts_file, 'r', encoding='utf-8') as file:
                data = yaml.safe_load(file)
                self._prompts = data.get('prompts', {})
                logger.info(f"Loaded {len(self._prompts)} prompts from {self.prompts_file}")
                
        except Exception as e:
            logger.error(f"Error loading prompts from {self.prompts_file}: {e}")
            self._prompts = self._get_default_prompts()
    
    def get_prompt(self, prompt_type: str) -> str:
        """Get a specific prompt by type"""
        prompt = self._prompts.get(prompt_type, "")
        if not prompt:
            logger.warning(f"Prompt '{prompt_type}' not found, using fallback")
            fallbacks = self._get_default_prompts()
            return fallbacks.get(prompt_type, "")
        return prompt
    
    def get_all_prompts(self) -> Dict[str, str]:
        """Get all available prompts"""
        return self._prompts.copy()
    
    def reload_prompts(self) -> None:
        """Reload prompts from file"""
        self.load_prompts()
    
    def _get_default_prompts(self) -> Dict[str, str]:
        """Fallback prompts if YAML file is not available"""
        return {
            "intent_detection": "Analyze the dialogue and extract booking intent in JSON format.",
            "service_identification": "Identify the service from the dialogue and return time_fractions in JSON.",
            "main_response": "Respond as Alina, the beauty salon AI assistant in JSON format.",
            "dialogue_compression": "Compress the dialogue keeping only essential booking information."
        }


# Global instance
prompt_loader = PromptLoader()


def get_prompt(prompt_type: str) -> str:
    """Convenience function to get a prompt"""
    return prompt_loader.get_prompt(prompt_type)


def get_all_prompts() -> Dict[str, str]:
    """Convenience function to get all prompts"""
    return prompt_loader.get_all_prompts()


def reload_prompts() -> None:
    """Convenience function to reload prompts"""
    prompt_loader.reload_prompts() 