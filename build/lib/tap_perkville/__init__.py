from tap_kit import main_method
from .executor import PerkvilleExecutor
from tap_kit import BaseClient
from .users import UsersStream

REQUIRED_CONFIG_KEYS = [
	"start_date",
	"client_id",
	"client_secret",
	"refresh_token",
	"access_token",
]

STREAMS = [
    UsersStream,
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