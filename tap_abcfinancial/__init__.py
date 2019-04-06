from tap_kit import main_method, BaseClient
from .streams import MembersStream, ProspectsStream, ClubsStream
from .executor import ABCExecutor


REQUIRED_CONFIG_KEYS = [
	"start_date",
	"api_key",
	"app_id",
]

STREAMS = [
	MembersStream,
	ProspectsStream,
	ClubsStream,
]


def main():
	main_method(
		REQUIRED_CONFIG_KEYS,
		ABCExecutor,
		BaseClient,
		STREAMS
	)


if __name__ == '__main__':
	main()
