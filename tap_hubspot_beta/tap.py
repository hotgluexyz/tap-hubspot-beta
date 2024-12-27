"""hubspot tap class."""

from typing import Any, Dict, List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError

from tap_hubspot_beta.client_v3 import hubspotV3Stream, DynamicDiscoveredHubspotV3Stream
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
    DiscoverCustomObjectsStream,
    CampaignsStream,
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
    CampaignsStream
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
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        try:
            discover_stream = DiscoverCustomObjectsStream(tap=self)
            for record in discover_stream.get_records(context={}):
                stream_class = self.generate_stream_class(record)
                streams.append(stream_class(tap=self))
        except FatalAPIError as exc:
            self.logger.info(f"failed to discover custom objects. Error={exc}")
        return streams

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

    def generate_stream_class(self, custom_object: Dict[str, Any]) -> hubspotV3Stream:
        # check for required fields to construct the custom objects class
        required_fields = ["id", "name", "objectTypeId", "properties"]
        errors = []
        for field in required_fields:
            if not custom_object.get(field):
                errors.append(f"Missing {field} in custom object.")
        if errors:
            errors.append(f"Failed custom_object={custom_object}.")
            error_msg = "\n".join(errors)
            raise ValueError(error_msg)

        name = custom_object.get("name")
        object_type_id = custom_object.get("objectTypeId")
        properties = custom_object.get("properties")
        
        if custom_object.get("archived", False):
            name = "archived_" + name
        class_name = name + "_Stream"
        class_name = "".join(word.capitalize() for word in class_name.split("_"))

        self.logger.info(f"Creating class {class_name}")

        return type(
            class_name,
            (DynamicDiscoveredHubspotV3Stream,),
            {
                "name": name,
                "path": f"crm/v3/objects/{object_type_id}/",
                "records_jsonpath": "$.results[*]",
                "primary_keys": ["id"],
                "replication_key": "updatedAt",
                "page_size": 100,
                "schema": self.generate_schema(properties),
                "is_custom_stream": True,
            },
        )

    def generate_schema(self, properties: List[Dict[str, Any]]) -> dict:
        properties_list = [
            th.Property("id", th.StringType),
            th.Property("updatedAt", th.DateTimeType), 
            th.Property("createdAt", th.DateTimeType), 
            th.Property("archived", th.BooleanType)
        ]
        main_properties = [p.name for p in properties_list]

        for property in properties:
            field_name = property.get("name")
            if field_name in main_properties:
                self.logger.info(f"Skipping field, it is a default field and already included.")
                continue
            if not field_name:
                self.logger.info(f"Skipping field without name.")
                continue
            th_type = hubspotV3Stream.extract_type(property, self.config.get("type_booleancheckbox_as_boolean"))
            properties_list.append(th.Property(field_name, th_type))
        return th.PropertiesList(*properties_list).to_dict()


if __name__ == "__main__":
    Taphubspot.cli()
