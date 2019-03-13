from tap_kit import main_method
from .executor import PerkvilleExecutor
from .client import BaseClient
from .users import UsersStream
from .connections import ConnectionsStream


REQUIRED_CONFIG_KEYS = [
	"start_date",
	"refresh_token",
	"access_token",
]

STREAMS = [
	UsersStream,
    ConnectionsStream,
]

def main():
	main_method(
		REQUIRED_CONFIG_KEYS,
		PerkvilleExecutor,
		BaseClient,
		STREAMS
	)

if __name__ == '__main__':
	main()