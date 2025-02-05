"""hubspot tap class."""

import os
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_hubspot_beta.streams import (
    AccountStream,
    AssociationDealsCompaniesStream,
    AssociationDealsContactsStream,
    AssociationDealsLineItemsStream,
    AssociationContactsCompaniesStream,
    AssociationContactsTicketsStream,
    CallsStream,
    CompaniesStream,
    ContactEventsStream,
    ContactListData,
    ContactListsStream,
    ContactsStream,
    DealsStream,
    EmailEventsStream,
    EmailsStream,
    FormsStream,
    FormSubmissionsStream,
    LineItemsStream,
    ArchivedLineItemsStream,
    ListsStream,
    MeetingsStream,
    NotesStream,
    OwnersStream,
    ProductsStream,
    TasksStream,
    EngagementStream,
    DealsPipelinesStream,
    ContactsV3Stream,
    TicketsStream,
    DispositionsStream,
    MarketingEmailsStream,
    ContactSubscriptionStatusStream,
    PostalMailStream,
    CommunicationsStream,
    QuotesStream,
    AssociationQuotesDealsStream,
    ListMembershipV3Stream,
    ListSearchV3Stream,
    ArchivedCompaniesStream,
    ArchivedDealsStream,
    DealsAssociationParent,
    FullsyncCompaniesStream,
    CurrenciesStream,
    AssociationMeetingsCompaniesStream,
    AssociationMeetingsContactsStream,
    AssociationMeetingsDealsStream,
    AssociationCallsCompaniesStream,
    AssociationCallsContactsStream,
    AssociationCallsDealsStream,
    AssociationCommunicationsCompaniesStream,
    AssociationCommunicationsContactsStream,
    AssociationCommunicationsDealsStream,
    AssociationEmailsCompaniesStream,
    AssociationEmailsContactsStream,
    AssociationEmailsDealsStream,
    AssociationNotesCompaniesStream,
    AssociationNotesContactsStream,
    AssociationNotesDealsStream,
    AssociationPostalMailCompaniesStream,
    AssociationPostalMailContactsStream,
    AssociationPostalMailDealsStream,
    AssociationTasksCompaniesStream,
    AssociationTasksContactsStream,
    AssociationTasksDealsStream,
    DealsHistoryPropertiesStream,
    ContactsHistoryPropertiesStream,
    ArchivedOwnersStream,
    ArchivedProductsStream,
)

 #When a new stream is added to the tap, it would break existing test suites.
# By allowing caller to ignore the stream we are able ensure existing tests continue to pass.
# 1. Get the environment variable IGNORE_STREAMS and split by commas
ignore_streams = os.environ.get('IGNORE_STREAMS', '').split(',')
print(f"IGNORE_STREAMS: "+ os.environ.get('IGNORE_STREAMS', ''))

# Function to add multiple streams to STREAM_TYPES if not in ignore_streams
def add_streams(stream_classes):

    stream_types = []
    for stream_class in stream_classes:
        if stream_class.__name__ not in ignore_streams:
            stream_types.append(stream_class)
        else:
            print(f"Ignored stream {stream_class.__name__} as it's in IGNORE_STREAMS.")
    return stream_types

STREAM_TYPES = add_streams([
    ContactsStream,
    ListsStream,
    CompaniesStream,
    DealsStream,
    OwnersStream,
    ContactListsStream,
    ContactListData,
    ProductsStream,
    LineItemsStream,
    ArchivedLineItemsStream,
    AssociationDealsContactsStream,
    AssociationDealsCompaniesStream,
    AssociationDealsLineItemsStream,
    AssociationContactsCompaniesStream,
    AssociationContactsTicketsStream,
    AccountStream,
    FormsStream,
    FormSubmissionsStream,
    ContactEventsStream,
    EmailEventsStream,
    EmailsStream,
    NotesStream,
    MeetingsStream,
    TasksStream,
    CallsStream,
    EngagementStream,
    DealsPipelinesStream,
    ContactsV3Stream,
    TicketsStream,
    DispositionsStream,
    MarketingEmailsStream,
    ContactSubscriptionStatusStream,
    PostalMailStream,
    CommunicationsStream,
    QuotesStream,
    AssociationQuotesDealsStream,
    ListMembershipV3Stream,
    ListSearchV3Stream,
    ArchivedCompaniesStream,
    ArchivedDealsStream,
    DealsAssociationParent,
    FullsyncCompaniesStream,
    CurrenciesStream,
    AssociationMeetingsCompaniesStream,
    AssociationMeetingsContactsStream,
    AssociationMeetingsDealsStream,
    AssociationCallsCompaniesStream,
    AssociationCallsContactsStream,
    AssociationCallsDealsStream,
    AssociationCommunicationsCompaniesStream,
    AssociationCommunicationsContactsStream,
    AssociationCommunicationsDealsStream,
    AssociationEmailsCompaniesStream,
    AssociationEmailsContactsStream,
    AssociationEmailsDealsStream,
    AssociationNotesCompaniesStream,
    AssociationNotesContactsStream,
    AssociationNotesDealsStream,
    AssociationPostalMailCompaniesStream,
    AssociationPostalMailContactsStream,
    AssociationPostalMailDealsStream,
    AssociationTasksCompaniesStream,
    AssociationTasksContactsStream,
    AssociationTasksDealsStream,
    DealsHistoryPropertiesStream,
    ContactsHistoryPropertiesStream,
    ArchivedOwnersStream,
    ArchivedProductsStream
])


class Taphubspot(Tap):
    """hubspot tap class."""

    name = "tap-hubspot"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("redirect_uri", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("expires_in", th.IntegerType),
        th.Property("start_date", th.DateTimeType),
        th.Property("access_token", th.StringType),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        catalog = super().catalog_dict
        streams = self.streams
        for stream in catalog["streams"]:
            stream_class = streams[stream["tap_stream_id"]]
            stream["stream_meta"] = {}
            if hasattr(stream_class, "load_fields_metadata") and stream["stream"] in ["deals", "lineitems", "contacts", "companies"]:
                stream_class.load_fields_metadata()
                for field in stream["schema"]["properties"]:
                    stream["schema"]["properties"][field]["field_meta"] = stream_class.fields_metadata.get(field, {})
        return catalog


if __name__ == "__main__":
    Taphubspot.cli()
