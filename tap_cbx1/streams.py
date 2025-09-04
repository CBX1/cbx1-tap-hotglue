from singer_sdk import typing as th
from tap_cbx1.client import CBX1Stream

product = th.ObjectType(
    th.Property("accountId", th.StringType),
    th.Property("accountStatus", th.StringType),
    th.Property("activityLevel", th.StringType),
    th.Property("buyingStage", th.StringType),
    th.Property("createdAt", th.DateTimeType),
    th.Property("createdBy", th.StringType),
    th.Property("dataUpdatedAt", th.DateTimeType),
    th.Property("crmAssociationId", th.StringType),
    th.Property("crmSystem", th.StringType),
    th.Property("fitScore", th.StringType),
    th.Property("id", th.StringType),
    th.Property("intentScore", th.IntegerType),
    th.Property("mostSearchedTopics", th.StringType),
    th.Property("productId", th.StringType),
    th.Property("spikes", th.StringType),
    th.Property("totalPageViews", th.IntegerType),
    th.Property("updatedAt", th.DateTimeType),
    th.Property("updatedBy", th.StringType),
    th.Property("visitorCount", th.IntegerType)
)

address = th.ObjectType(
    th.Property("buildingOrStreet", th.StringType),
    th.Property("city", th.StringType),
    th.Property("state", th.StringType),
    th.Property("zipCode", th.StringType),
    th.Property("country", th.StringType)
)

class AccountStream(CBX1Stream):
    name = "accounts"
    path = "/targets/accounts"
    primary_keys = ["id"]
    
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("updatedBy", th.StringType),
        th.Property("dataUpdatedAt", th.DateTimeType),
        th.Property("crmAssociationId", th.StringType),
        th.Property("crmSystem", th.StringType),
        th.Property("name", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("subIndustry", th.StringType),
        th.Property("domain", th.StringType),
        th.Property("summary", th.StringType),
        th.Property("foundingYear", th.StringType),
        th.Property("industries", th.ArrayType(th.StringType)),
        th.Property("subIndustries", th.ArrayType(th.StringType)),
        th.Property("annualRevenue", th.IntegerType),
        th.Property("annualRevenueRange", th.StringType),
        th.Property("employeeCount", th.IntegerType),
        th.Property("employeeRange", th.StringType),
        th.Property("hqLocation", th.ObjectType(address)),
        th.Property("ownershipType", th.StringType),
        th.Property("businessModel", th.StringType),
        th.Property("fundingHistory", th.StringType),
        th.Property("totalFunding", th.IntegerType),
        th.Property("allInvestors", th.ArrayType(th.StringType)),
        th.Property("linkedinUrl", th.StringType),
        th.Property("twitterUrl", th.StringType),
        th.Property("facebookUrl", th.StringType),
        th.Property("logoUrl", th.StringType),
        th.Property("products", th.ArrayType(product)),
    ).to_dict()



class ContactStream(CBX1Stream):
    name = "contacts"
    path = "/targets/contacts"
    primary_keys = ["id"]

    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("createdBy", th.StringType),
        th.Property("updatedBy", th.StringType),
        th.Property("dataUpdatedAt", th.DateTimeType),
        th.Property("crmAssociationId", th.StringType),
        th.Property("crmSystem", th.StringType),
        th.Property("accountId", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("middleName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("salutation", th.StringType),
        th.Property("managementLevel", th.StringType),
        th.Property("department", th.StringType),
        th.Property("email", th.StringType),
        th.Property("secondaryEmail", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("secondaryPhone", th.StringType),
        th.Property("title", th.StringType),
        th.Property("address", address),
        th.Property("linkedinUrl", th.StringType),
        th.Property("previousCompanyName", th.StringType),
        th.Property("previousCompanyLinkedInUrl", th.StringType)
    ).to_dict()

