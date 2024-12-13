"""hubspot tap class."""

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
    ContactsAssociationStream,
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
    DealsHistoryPropertiesStream,
    ContactsHistoryPropertiesStream,
    LeadsStream
)

STREAM_TYPES = [
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
    ContactsAssociationStream,
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
    DealsHistoryPropertiesStream,
    ContactsHistoryPropertiesStream,
    LeadsStream
]


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
        if self.config.get("catalog_metadata", False):
            streams = self.streams
            for stream in catalog["streams"]:
                stream_class = streams[stream["tap_stream_id"]]
                stream["stream_meta"] = {}
                if hasattr(stream_class, "load_fields_metadata"):
                    stream_class.load_fields_metadata()
                    for field in stream["schema"]["properties"]:
                        stream["schema"]["properties"][field]["field_meta"] = stream_class.fields_metadata.get(field, {})
        return catalog


if __name__ == "__main__":
    Taphubspot.cli()
