"""
Logger setup and configuration.
Provides consistent logging across the application.
"""

import logging

def setup_logger(name, log_level=logging.INFO):
    """
    Set up and configure a logger.
    
    Args:
        name (str): Name of the logger
        log_level (int, optional): Logging level. Defaults to logging.INFO.
        
    Returns:
        Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create console handler if no handlers exist
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
    
    return logger
