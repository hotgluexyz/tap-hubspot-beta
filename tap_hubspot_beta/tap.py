"""hubspot tap class."""

import os
from typing import List, Dict, Type 
import logging
from singer_sdk.helpers._compat import final

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
    FullsyncDealsStream,
    SessionAnalyticsDailyReportsStream,
    SessionAnalyticsWeeklyReportsStream,
    SessionAnalyticsMonthlyReportsStream,
    SessionAnalyticsTotalReportStream,
    BreakdownsAnalyticsReportsGeolocationMonthlyStream,
    BreakdownsAnalyticsReportsGeolocationTotalStream,
    BreakdownsAnalyticsReportsSourcesMonthlyStream,
    BreakdownsAnalyticsReportsSourcesTotalStream,
    BreakdownsAnalyticsReportsUtmCampaignsMonthlyStream,
    BreakdownsAnalyticsReportsUtmCampaignsTotalStream,
    FormsSummaryMonthlyStream,
    TeamsStream,
    MeetingsAssociationStream,
    CallsAssociationStream,
    CommunicationsAssociationStream,
    EmailsAssociationStream,
    NotesAssociationStream,
    PostalAssociationStream,
    TasksAssociationStream,
    FormsAllStream,
    SourcesSummaryMonthlyStream,
    PagesSummaryMonthlyStream,
    LandingPagesSummaryMonthlyStream,
    LandingPagesStream,
    UtmCampaignSummaryMonthlyStream,
    GeolocationSummaryMonthlyStream,
    LeadsStream,
    FullsyncContactsV3Stream,
    ArchivedContactsStream,
    ListLegacyIdsStream
)

 #When a new stream is added to the tap, it would break existing test suites.
# By allowing caller to ignore the stream we are able ensure existing tests continue to pass.
# 1. Get the environment variable IGNORE_STREAMS and split by commas
ignore_streams = os.environ.get('IGNORE_STREAMS', '').split(',')
logging.info(f"IGNORE_STREAMS: "+ os.environ.get('IGNORE_STREAMS', ''))

# 2. Get the environment variable INCLUDE_STREAMS and split by commas
include_streams = os.environ.get('INCLUDE_STREAMS', "").split(',') if os.environ.get('INCLUDE_STREAMS', "") else []
logging.info(f"INCLUDE_STREAMS: "+ os.environ.get('INCLUDE_STREAMS', ''))

# Function to add streams to STREAM_TYPES if INCLUDE_STREAMS is set, otherwise add streams to STREAM_TYPES if not in IGNORE_STREAMS
def add_streams(stream_classes):
    stream_types = []
    for stream_class in stream_classes:
        if include_streams:
            if stream_class.__name__ not in include_streams:
                logging.info(f"Ignored stream {stream_class.__name__} as it's not in INCLUDE_STREAMS.")
                continue
            stream_types.append(stream_class)
        else:
            if stream_class.__name__ not in ignore_streams:
                stream_types.append(stream_class)
            else:
                logging.info(f"Ignored stream {stream_class.__name__} as it's in IGNORE_STREAMS.")
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
    ArchivedProductsStream,
    FullsyncDealsStream,
    SessionAnalyticsDailyReportsStream,
    SessionAnalyticsWeeklyReportsStream,
    SessionAnalyticsMonthlyReportsStream,
    SessionAnalyticsTotalReportStream,
    BreakdownsAnalyticsReportsGeolocationMonthlyStream,
    BreakdownsAnalyticsReportsGeolocationTotalStream,
    BreakdownsAnalyticsReportsSourcesMonthlyStream,
    BreakdownsAnalyticsReportsSourcesTotalStream,
    BreakdownsAnalyticsReportsUtmCampaignsMonthlyStream,
    BreakdownsAnalyticsReportsUtmCampaignsTotalStream,
    FormsSummaryMonthlyStream,
    TeamsStream,
    MeetingsAssociationStream,
    CallsAssociationStream,
    CommunicationsAssociationStream,
    EmailsAssociationStream,
    NotesAssociationStream,
    PostalAssociationStream,
    TasksAssociationStream,
    FormsAllStream,
    SourcesSummaryMonthlyStream,
    PagesSummaryMonthlyStream,
    LandingPagesSummaryMonthlyStream,
    LandingPagesStream,
    UtmCampaignSummaryMonthlyStream,
    GeolocationSummaryMonthlyStream,
    LeadsStream,
    FullsyncContactsV3Stream,
    ArchivedContactsStream,
    ListLegacyIdsStream
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
        th.Property("enable_list_selection", th.BooleanType, default=False),

    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        stream_types = list(STREAM_TYPES)

        # If enable_list_selection is false, remove ContactListsStream and ContactListData from the list
        if not self.config.get("enable_list_selection"):
            stream_types = [
                st for st in stream_types
                if st.__name__ != "ContactListsStream" and st.__name__ != "ContactListData"
            ]

        # Instantiate them
        return [stream_class(tap=self) for stream_class in stream_types]

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

    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type:
                parents = streams_by_type[stream_type.parent_stream_type]

                # assign parent stream dynamically, if stream has parent attribute
                if hasattr(stream_type, "parent"):
                    new_parent = [streams_by_type[stream][0] for stream in streams_by_type if stream.name == streams[0].parent]
                    if new_parent and new_parent != parents:
                        parents = new_parent

                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )

        streams = [stream for streams in streams_by_type.values() for stream in streams]
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )


if __name__ == "__main__":
    Taphubspot.cli()
