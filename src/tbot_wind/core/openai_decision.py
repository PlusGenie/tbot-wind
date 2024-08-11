import asyncio
import openai
from loguru import logger

from ..utils.tbot_env import shared


class OpenAIDecisionEngine:
    def __init__(self):
        """
        Initializes the decision engine with OpenAI API key and model name for signal interpretation.
        """
        self.api_key = shared.openai_key
        self.model_name = shared.openai_model

        self.enabled = True
        if not self.api_key or not self.model_name:
            logger.info(
                "OpenAI API key or mode is not set. Please set them in the environment variable."
            )
            self.enabled = False
            return

        # Set OpenAI API key
        openai.api_key = self.api_key

    async def analyze_signal(self, signal_data: dict) -> dict:
        """
        Calls the OpenAI model to analyze signal data and determine buy/sell signals
        using the reasoning model. This method uses asyncio to handle the API call with a 2-second timeout.

        Args:
        - signal_data (dict): Dictionary containing the financial signals.

        Returns:
        - dict: A decision object with keys `buy_signal`, `sell_signal`, and `interpretation`.
        """
        if not self.enabled:
            return {"interpretation": "No interpretation available."}

        try:
            logger.info(
                "Sending signal data to OpenAI for buy/sell decision using reasoning model."
            )

            # Format the signal data into a prompt string
            formatted_data = self._format_signal_data(signal_data)

            # Use asyncio's wait_for to enforce the timeout
            try:
                response_text = await asyncio.wait_for(
                    self._call_openai_api(formatted_data), timeout=2
                )
                decision = self._process_response(response_text)
                logger.success(f"Received decision from OpenAI: {decision}")
                return decision
            except asyncio.TimeoutError:
                logger.error("OpenAI API call timed out.")
                return {"buy_signal": False, "sell_signal": False, "interpretation": "Timeout error"}

        except Exception as e:
            logger.error(f"Error while calling OpenAI API: {e}")
            return {"buy_signal": False, "sell_signal": False, "interpretation": "Error in decision making"}

    def _format_signal_data(self, signal_data: dict) -> str:
        """
        Formats the signal data into a suitable string for OpenAI API prompt.
        Limits the number of signals to a maximum of 4.

        Args:
        - signal_data (dict): Input signal data as a dictionary.

        Returns:
        - str: Formatted data as a string suitable for OpenAI prompt.
        """
        # Limit the number of indicators to a maximum of 4
        limited_signals = {
            key: signal_data[key] for key in list(signal_data.keys())[:4]
        }

        # Ensure the prompt is clear and directs OpenAI to make a specific buy/sell decision
        prompt_str = (
            f"Analyze the following market signals: {', '.join([f'{key}: {value}' for key, value in limited_signals.items()])}. "
            "Based on these indicators, should the trader take a BUY or SELL action? Please provide a one-word decision ('buy' or 'sell') and a one-line summary."
        )
        return prompt_str

    def _process_response(self, text_response: str) -> dict:
        """
        Processes the OpenAI API response to extract the decision.

        Args:
        - text_response: OpenAI API response as text.

        Returns:
        - dict: A dictionary containing the buy/sell signal and explanation.
        """
        text_response = text_response.strip().lower()

        # Check if the response indicates a buy or sell signal
        buy_signal = "buy" in text_response
        sell_signal = "sell" in text_response

        return {
            "buy_signal": buy_signal,
            "sell_signal": sell_signal,
            "interpretation": text_response,  # The one-line summary provided by OpenAI
        }

    async def _call_openai_api(self, prompt: str) -> str:
        """
        Calls the OpenAI API with the given model and prompt. This function is responsible for making the API call.

        Args:
        - model_name (str): The name of the OpenAI model to use.
        - prompt (str): The prompt to send to OpenAI.

        Returns:
        - str: The content of the response message from OpenAI.
        """
        if not self.enabled:
            return ""

        response = openai.chat.completions.create(
            model=self.model_name,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            max_completion_tokens=1000,  # This controls both reasoning and completion tokens.
        )
        return response.choices[0].message.content
