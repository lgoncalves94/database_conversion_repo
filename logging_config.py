import logging

# Configure the logging settings
logging.basicConfig(
    level=logging.WARNING,  # Adjust the log level as needed (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        #logging.FileHandler('data_exploration.log'),   <- Uncomment to implement File logging
        logging.StreamHandler()  # Also output to console
    ]
)

# Create a logger object
log = logging.getLogger()
