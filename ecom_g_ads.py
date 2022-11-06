# import pandas as pd
# import numpy as np
#
# import argparse
import sys
import uuid
import datetime
from google.api_core import protobuf_helpers
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


class GAdsEcomru:
    def __init__(self,
                 client_id,
                 client_secret,
                 developer_token,
                 login_customer_id,
                 refresh_token):

        self.use_proto_plus = True
        self.customer_id = login_customer_id
        credentials = {
            "client_id": client_id,
            "client_secret": client_secret,
            "developer_token": developer_token,
            "login_customer_id": login_customer_id,
            "refresh_token": refresh_token,
            "use_proto_plus": self.use_proto_plus}
        try:
            self.client = GoogleAdsClient.load_from_dict(credentials)
            # self.client = GoogleAdsClient.load_from_storage("google-ads-intapichecking@gmail.com.yaml")
        except:
            self.client = None
            print('Ошибка при инициализации клиента')

    def get_campaigns(self, customer_id: str):
        """
        Возвращает id и названия кампаний
        """
        try:
            ga_service = self.client.get_service("GoogleAdsService")
            query = """
                SELECT
                  campaign.id,
                  campaign.name
                FROM campaign
                ORDER BY campaign.id
                """
            stream = ga_service.search_stream(customer_id=customer_id, query=query)
            result = []
            for batch in stream:
                for row in batch.results:
                    result.append({'campaign_id': row.campaign.id,
                                   'campaign_name': row.campaign.name})
            return result
        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def get_ad_groups(self, customer_id, page_size=1000, campaign_id=None):
        """
        Получить группы
        """
        try:
            ga_service = self.client.get_service("GoogleAdsService")
            query = """
                SELECT
                  campaign.id,
                  ad_group.id,
                  ad_group.name
                FROM ad_group"""
            if campaign_id:
                query += f" WHERE campaign.id = {campaign_id}"
            search_request = self.client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            search_request.page_size = page_size
            results = ga_service.search(request=search_request)
            res=[]
            for row in results:
                print(
                    f"Ad group with ID {row.ad_group.id} and name "
                    f'"{row.ad_group.name}" was found in campaign with '
                    f"ID {row.campaign.id}.")
                res.append({'ad_group_id': row.ad_group.id,
                            'ad_group_name': row.ad_group.name,
                            'campaign_id': row.campaign.id})
            return res

        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def get_keywords(self, customer_id,
                     omit_unselected_resource_names=False,
                     ad_group_id=None,
                     page_size=1000):
        """
        Получить ключевые слова
        """
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = """
                SELECT
                  ad_group.id,
                  ad_group_criterion.type,
                  ad_group_criterion.criterion_id,
                  ad_group_criterion.keyword.text,
                  ad_group_criterion.keyword.match_type
                FROM ad_group_criterion
                WHERE ad_group_criterion.type = KEYWORD"""

            if ad_group_id:
                query += f" AND ad_group.id = {ad_group_id}"

            # Adds omit_unselected_resource_names=true to the PARAMETERS clause of the
            # Google Ads Query Language (GAQL) query, which excludes the resource names
            # of all resources that aren't explicitly requested in the SELECT clause.
            # Enabling this option reduces payload size, but if you plan to use a
            # returned object in subsequent mutate operations, make sure you explicitly
            # request its "resource_name" field in the SELECT clause.
            #
            # Read more about PARAMETERS:
            # https://developers.google.com/google-ads/api/docs/query/structure#parameters
            if omit_unselected_resource_names:
                query += " PARAMETERS omit_unselected_resource_names=true"

            search_request = self.client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            search_request.page_size = page_size

            results = ga_service.search(request=search_request)

            res=[]
            for row in results:
                ad_group = row.ad_group
                ad_group_criterion = row.ad_group_criterion
                keyword = row.ad_group_criterion.keyword

                if omit_unselected_resource_names:
                    resource_name_log_statement = ""
                else:
                    resource_name_log_statement = (
                        f" and resource name '{ad_group.resource_name}'"
                    )

                print(
                    f'Keyword with text "{keyword.text}", match type '
                    f"{keyword.match_type}, criteria type "
                    f"{ad_group_criterion.type_}, and ID "
                    f"{ad_group_criterion.criterion_id} was found in ad group "
                    f"with ID {ad_group.id}{resource_name_log_statement}."
                )
                res.append({'keyword_text': keyword.text,
                            'match_type': keyword.match_type.name,
                            'criteria_type': ad_group_criterion.type_.name,
                            'criterion_id': ad_group_criterion.criterion_id,
                            'ad_group_id': ad_group.id,
                            'resource_name_log_statement': resource_name_log_statement})
            return res

        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def create_keyword_plan(self, customer_id,
                            keyword_plan_name='Keyword plan for traffic estimate'):
        """Adds a keyword plan to the given customer account.
        Args:
            customer_id: A str of the customer_id to use in requests.
            keyword_plan_name: name.
        Returns:
            A str of the resource_name for the newly created keyword plan.
        Raises:
            GoogleAdsException: If an error is returned from the API.
        """
        keyword_plan_service = self.client.get_service("KeywordPlanService")
        operation = self.client.get_type("KeywordPlanOperation")
        keyword_plan = operation.create
        keyword_plan.name = f"{keyword_plan_name} {uuid.uuid4()}"
        forecast_interval = self.client.enums.KeywordPlanForecastIntervalEnum.NEXT_QUARTER
        keyword_plan.forecast_period.date_interval = forecast_interval
        response = keyword_plan_service.mutate_keyword_plans(customer_id=customer_id, operations=[operation])
        resource_name = response.results[0].resource_name
        print(f"Created keyword plan with resource name: {resource_name}")
        return resource_name

    def create_keyword_plan_campaign(self, customer_id, keyword_plan, language="languageConstants/1000",
                                     keyword_plan_campaign_name='Keyword plan campaign'):
        """Adds a keyword plan campaign to the given keyword plan.
        Args:
            customer_id: A str of the customer_id to use in requests.
            keyword_plan: A str of the keyword plan resource_name this keyword plan
                campaign should be attributed to.create_keyword_plan.
            language: language constant, english as default.
            keyword_plan_campaign_name: name.
        Returns:
            A str of the resource_name for the newly created keyword plan campaign.
        Raises:
            GoogleAdsException: If an error is returned from the API.
        """
        keyword_plan_campaign_service = self.client.get_service("KeywordPlanCampaignService")
        operation = self.client.get_type("KeywordPlanCampaignOperation")
        keyword_plan_campaign = operation.create
        keyword_plan_campaign.name = f"{keyword_plan_campaign_name} {uuid.uuid4()}"
        keyword_plan_campaign.cpc_bid_micros = 1000000
        keyword_plan_campaign.keyword_plan = keyword_plan
        network = self.client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
        keyword_plan_campaign.keyword_plan_network = network
        geo_target = self.client.get_type("KeywordPlanGeoTarget")
        # Constant for U.S. Other geo target constants can be referenced here:
        # https://developers.google.com/google-ads/api/reference/data/geotargets
        geo_target.geo_target_constant = "geoTargetConstants/2840"
        keyword_plan_campaign.geo_targets.append(geo_target)
        language = language
        keyword_plan_campaign.language_constants.append(language)
        response = keyword_plan_campaign_service.mutate_keyword_plan_campaigns(customer_id=customer_id,
                                                                               operations=[operation])
        resource_name = response.results[0].resource_name
        print(f"Created keyword plan campaign with resource name: {resource_name}")
        return resource_name

    def create_keyword_plan_ad_group(self, customer_id, keyword_plan_campaign,
                                     keyword_plan_ad_group_name='Keyword plan ad group'):
        """Adds a keyword plan ad group to the given keyword plan campaign.
        Args:
            customer_id: A str of the customer_id to use in requests.
            keyword_plan_campaign: A str of the keyword plan campaign resource_name
                this keyword plan ad group should be attributed to.
            keyword_plan_ad_group_name: name.
        Returns:
            A str of the resource_name for the newly created keyword plan ad group.
        Raises:
            GoogleAdsException: If an error is returned from the API.
        """
        operation = self.client.get_type("KeywordPlanAdGroupOperation")
        keyword_plan_ad_group = operation.create
        keyword_plan_ad_group.name = f"{keyword_plan_ad_group_name} {uuid.uuid4()}"
        keyword_plan_ad_group.cpc_bid_micros = 2500000
        keyword_plan_ad_group.keyword_plan_campaign = keyword_plan_campaign
        keyword_plan_ad_group_service = self.client.get_service("KeywordPlanAdGroupService")
        response = keyword_plan_ad_group_service.mutate_keyword_plan_ad_groups(customer_id=customer_id,
                                                                               operations=[operation])
        resource_name = response.results[0].resource_name
        print(f"Created keyword plan ad group with resource name: {resource_name}")
        return resource_name

    def create_keyword_plan_ad_group_keywords(self, customer_id,
                                              plan_ad_group,
                                              keywords: list,
                                              bids: list
                                              ):
        """Adds keyword plan ad group keywords to the given keyword plan ad group.
        Args:
            customer_id: A str of the customer_id to use in requests.
            plan_ad_group: A str of the keyword plan ad group resource_name
                these keyword plan keywords should be attributed to.
            keywords: list of keywords.
            bids: list of bids.
        Raises:
            GoogleAdsException: If an error is returned from the API.
        """
        keyword_plan_ad_group_keyword_service = self.client.get_service("KeywordPlanAdGroupKeywordService")
        # operation = self.client.get_type("KeywordPlanAdGroupKeywordOperation")
        operations = []
        if len(keywords)==len(bids):
            for keyword, bid in zip(keywords, bids):
                operation = self.client.get_type("KeywordPlanAdGroupKeywordOperation")
                keyword_plan_ad_group_keyword1 = operation.create
                keyword_plan_ad_group_keyword1.text = keyword
                keyword_plan_ad_group_keyword1.cpc_bid_micros = int(bid)*1e6
                keyword_plan_ad_group_keyword1.match_type = self.client.enums.KeywordMatchTypeEnum.BROAD
                keyword_plan_ad_group_keyword1.keyword_plan_ad_group = plan_ad_group
                operations.append(operation)

            response = keyword_plan_ad_group_keyword_service.mutate_keyword_plan_ad_group_keywords(customer_id=customer_id,
                                                                                                   operations=operations)
            for result in response.results:
                print("Created keyword plan ad group keyword with resource name: "
                      f"{result.resource_name}")
        else:
            print("Input data incorrect")

    def create_keyword_plan_negative_campaign_keywords(self, customer_id, plan_campaign, negative_keywords: list):
        """Adds a keyword plan negative campaign keyword to the given campaign.
        Args:
            customer_id: A str of the customer_id to use in requests.
            plan_campaign: A str of the keyword plan campaign resource_name
                this keyword plan negative keyword should be attributed to.
            negative_keywords: list of negative keywords
        Raises:
            GoogleAdsException: If an error is returned from the API.
        """
        keyword_plan_negative_keyword_service = self.client.get_service("KeywordPlanCampaignKeywordService")
        operations = []
        for keyword in negative_keywords:
            operation = self.client.get_type("KeywordPlanCampaignKeywordOperation")
            keyword_plan_campaign_keyword = operation.create
            keyword_plan_campaign_keyword.text = keyword
            keyword_plan_campaign_keyword.match_type = self.client.enums.KeywordMatchTypeEnum.BROAD
            keyword_plan_campaign_keyword.keyword_plan_campaign = plan_campaign
            keyword_plan_campaign_keyword.negative = True
            operations.append(operation)

        response = keyword_plan_negative_keyword_service.mutate_keyword_plan_campaign_keywords(customer_id=customer_id,
                                                                                               operations=operations)

        print("Created keyword plan campaign keyword with resource name: "
              f"{response.results[0].resource_name}")

    def add_keyword_plan(self, customer_id, keywords: list, bids: list, negative_keywords=None,
                         language="languageConstants/1000",
                         keyword_plan_name='Keyword plan for traffic estimate',
                         keyword_plan_campaign_name='Keyword plan campaign',
                         keyword_plan_ad_group_name='Keyword plan ad group'):
        """Adds a keyword plan, campaign, ad group, etc. to the customer account.
        Args:
            customer_id: A str of the customer_id to use in requests.
            keywords: list of keywords.
            bids: list of bids.
            negative_keywords: list of negative keywords.
            language: language constant, english as default.
            keyword_plan_name: name.
            keyword_plan_campaign_name: name.
            keyword_plan_ad_group_name: name.
        Raises:
            GoogleAdsException: If an error is returned from the API.
        """
        try:
            keyword_plan = self.create_keyword_plan(customer_id, keyword_plan_name)
            keyword_plan_campaign = self.create_keyword_plan_campaign(customer_id, keyword_plan, language,
                                                                      keyword_plan_campaign_name)
            keyword_plan_ad_group = self.create_keyword_plan_ad_group(customer_id, keyword_plan_campaign,
                                                                      keyword_plan_ad_group_name)
            self.create_keyword_plan_ad_group_keywords(customer_id, keyword_plan_ad_group, keywords, bids)
            if negative_keywords is not None:
                self.create_keyword_plan_negative_campaign_keywords(customer_id, keyword_plan_campaign,
                                                                    negative_keywords)
            return 'OK'

        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def create_ad_group(self, customer_id, campaign_id, name: str, bid: int):
        """
        Создает группу объявлений
        """
        try:
            ad_group_service = self.client.get_service("AdGroupService")
            campaign_service = self.client.get_service("CampaignService")

            # Create ad group.
            ad_group_operation = self.client.get_type("AdGroupOperation")
            ad_group = ad_group_operation.create
            ad_group.name = f"{name} {uuid.uuid4()}"
            ad_group.status = self.client.enums.AdGroupStatusEnum.ENABLED
            ad_group.campaign = campaign_service.campaign_path(customer_id, campaign_id)
            ad_group.type_ = self.client.enums.AdGroupTypeEnum.SEARCH_STANDARD
            ad_group.cpc_bid_micros = int(bid)*1e6

            # Add the ad group.
            ad_group_response = ad_group_service.mutate_ad_groups(customer_id=customer_id,
                                                                  operations=[ad_group_operation])
            print(f"Created ad group {ad_group_response.results[0].resource_name}.")
            return f'{ad_group_response.results[0].resource_name}'

        except GoogleAdsException as ex:
            print(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" and includes the following errors:'
            )
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def add_campaign(self, customer_id,
                     campaign_budget_name: str,
                     campaign_budget_amount_micros: int,
                     campaign_name: str,
                     date_from,
                     date_to,
                     enhanced_cpc_enabled=True,
                     target_google_search=True,
                     target_search_network=True,
                     target_partner_search_network=False,
                     target_content_network=True
                     ):
        """
        Добавить кампанию
        """

        def handle_googleads_exception(exception):
            print(f'Request with ID "{exception.request_id}" failed with status '
                  f'"{exception.error.code().name}" and includes the following errors:')
            for error in exception.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            sys.exit(1)

        campaign_budget_service = self.client.get_service("CampaignBudgetService")
        campaign_service = self.client.get_service("CampaignService")

        # Create a budget, which can be shared by multiple campaigns.
        campaign_budget_operation = self.client.get_type("CampaignBudgetOperation")
        campaign_budget = campaign_budget_operation.create
        campaign_budget.name = f"{campaign_budget_name} {uuid.uuid4()}"
        campaign_budget.delivery_method = self.client.enums.BudgetDeliveryMethodEnum.STANDARD
        campaign_budget.amount_micros = campaign_budget_amount_micros

        try:
            # Add budget.
            campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(customer_id=customer_id,
                                                                                       operations=[campaign_budget_operation])

            # Create campaign.
            campaign_operation = self.client.get_type("CampaignOperation")
            campaign = campaign_operation.create
            campaign.name = f"{campaign_name} {uuid.uuid4()}"
            campaign.advertising_channel_type = self.client.enums.AdvertisingChannelTypeEnum.SEARCH

            # Recommendation: Set the campaign to PAUSED when creating it to prevent
            # the ads from immediately serving. Set to ENABLED once you've added
            # targeting and the ads are ready to serve.
            campaign.status = self.client.enums.CampaignStatusEnum.PAUSED

            # Set the bidding strategy and budget.
            campaign.manual_cpc.enhanced_cpc_enabled = enhanced_cpc_enabled
            campaign.campaign_budget = campaign_budget_response.results[0].resource_name

            # Set the campaign network options.
            campaign.network_settings.target_google_search = target_google_search
            campaign.network_settings.target_search_network = target_search_network
            campaign.network_settings.target_partner_search_network = target_partner_search_network
            # Enable Display Expansion on Search campaigns. For more details see:
            # https://support.google.com/google-ads/answer/7193800
            campaign.network_settings.target_content_network = target_content_network

            # Optional: Set the start date.
            # start_time = datetime.date.today() + datetime.timedelta(days=1)
            start_time = datetime.datetime.strptime(date_from, '%Y-%m-%d').date()
            campaign.start_date = datetime.date.strftime(start_time, "%Y%m%d")

            # Optional: Set the end date.
            # end_time = start_time + datetime.timedelta(weeks=4)
            end_time = datetime.datetime.strptime(date_to, '%Y-%m-%d').date()
            campaign.end_date = datetime.date.strftime(end_time, "%Y%m%d")

            # Add the campaign.
            try:
                campaign_response = campaign_service.mutate_campaigns(customer_id=customer_id,
                                                                      operations=[campaign_operation])
                print(f"Created campaign {campaign_response.results[0].resource_name}.")
                return 'OK'
            except GoogleAdsException as ex:
                handle_googleads_exception(ex)

        except GoogleAdsException as ex:
            handle_googleads_exception(ex)

    def add_keyword(self, customer_id, ad_group_id, keyword_text, negative=False, url=None):
        """
        Добавить ключевые слова
        """
        try:
            ad_group_service = self.client.get_service("AdGroupService")
            ad_group_criterion_service = self.client.get_service("AdGroupCriterionService")

            # Create keyword.
            ad_group_criterion_operation = self.client.get_type("AdGroupCriterionOperation")
            ad_group_criterion = ad_group_criterion_operation.create
            ad_group_criterion.ad_group = ad_group_service.ad_group_path(customer_id, ad_group_id)
            ad_group_criterion.status = self.client.enums.AdGroupCriterionStatusEnum.ENABLED
            ad_group_criterion.keyword.text = keyword_text
            ad_group_criterion.keyword.match_type = self.client.enums.KeywordMatchTypeEnum.EXACT

            # Optional field
            # All fields can be referenced from the protos directly.
            # The protos are located in subdirectories under:
            # https://github.com/googleapis/googleapis/tree/master/google/ads/googleads
            ad_group_criterion.negative = negative

            # Optional repeated field
            if url is not None:
                ad_group_criterion.final_urls.append(url)

            # Add keyword
            ad_group_criterion_response = (
                ad_group_criterion_service.mutate_ad_group_criteria(customer_id=customer_id,
                                                                    operations=[ad_group_criterion_operation])
            )
            print("Created keyword "
                  f"{ad_group_criterion_response.results[0].resource_name}.")
            return 'OK'

        except GoogleAdsException as ex:
            print(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" and includes the following errors:'
            )
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def add_responsive_search_ad(self, customer_id,
                                 ad_group_id,
                                 final_url,
                                 headlines_,
                                 descriptions_,
                                 text_p1=None,
                                 text_p2=None,
                                 pinned_headline_=False
                                 ):
        """
        Добавить адаптивное поисковое объявление
        """

        def create_ad_text_asset(client, text, pinned_field=None):
            """Create an AdTextAsset."""
            ad_text_asset = client.get_type("AdTextAsset")
            ad_text_asset.text = text
            if pinned_field:
                ad_text_asset.pinned_field = pinned_field
            return ad_text_asset

        try:
            ad_group_ad_service = self.client.get_service("AdGroupAdService")
            ad_group_service = self.client.get_service("AdGroupService")

            # Create the ad group ad.
            ad_group_ad_operation = self.client.get_type("AdGroupAdOperation")
            ad_group_ad = ad_group_ad_operation.create
            ad_group_ad.status = self.client.enums.AdGroupAdStatusEnum.PAUSED
            ad_group_ad.ad_group = ad_group_service.ad_group_path(customer_id, ad_group_id)

            # Set responsive search ad info.
            ad_group_ad.ad.final_urls.append(final_url)

            headlines_ = headlines_[:15]
            descriptions_ = descriptions_[:4]
            if max([len(headline_) for headline_ in headlines_]) < 30 and max([len(description_)
                                                                               for description_ in descriptions_]) < 90:
                # Set a pinning to always choose this asset for HEADLINE_1. Pinning is
                # optional; if no pinning is set, then headlines and descriptions will be
                # rotated and the ones that perform best will be used more often.
                if pinned_headline_ is True:
                    served_asset_enum = self.client.enums.ServedAssetFieldTypeEnum.HEADLINE_1
                    pinned_headline = create_ad_text_asset(self.client, headlines_[0], served_asset_enum)
                    headlines = [pinned_headline] + [create_ad_text_asset(self.client, headline_)
                                                     for headline_ in headlines_[1:]]
                    ad_group_ad.ad.responsive_search_ad.headlines.extend(headlines)
                else:
                    headlines = [create_ad_text_asset(self.client, headline_) for headline_ in headlines_]
                    ad_group_ad.ad.responsive_search_ad.headlines.extend(headlines)

                descriptions = [create_ad_text_asset(self.client, description_) for description_ in descriptions_]
                ad_group_ad.ad.responsive_search_ad.descriptions.extend(descriptions)

                if text_p1 is not None:
                    if len(text_p1) <= 15:
                        ad_group_ad.ad.responsive_search_ad.path1 = text_p1
                    if text_p2 is not None:
                        if len(text_p1) <= 15:
                            ad_group_ad.ad.responsive_search_ad.path2 = text_p2

                # Send a request to the server to add a responsive search ad.
                ad_group_ad_response = ad_group_ad_service.mutate_ad_group_ads(
                    customer_id=customer_id, operations=[ad_group_ad_operation]
                )

                for result in ad_group_ad_response.results:
                    print(
                        f"Created responsive search ad with resource name "
                        f'"{result.resource_name}".'
                    )
                return 'OK'

            else:
                print('Превышена длина заголовка или описания')
                return None

        except GoogleAdsException as ex:
            print(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" and includes the following errors:'
            )
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def get_expanded_text_ads(self, customer_id, ad_group_id=None):
        """
        Получить развернутые текстовые объявления
        """
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = """
                SELECT
                  ad_group.id,
                  ad_group_ad.ad.id,
                  ad_group_ad.ad.expanded_text_ad.headline_part1,
                  ad_group_ad.ad.expanded_text_ad.headline_part2,
                  ad_group_ad.status
                FROM ad_group_ad
                WHERE ad_group_ad.ad.type = EXPANDED_TEXT_AD"""

            if ad_group_id:
                query += f" AND ad_group.id = {ad_group_id}"

            stream = ga_service.search_stream(customer_id=customer_id, query=query)

            result = []
            for batch in stream:
                for row in batch.results:
                    ad = row.ad_group_ad.ad

                    if ad.expanded_text_ad:
                        expanded_text_ad_info = ad.expanded_text_ad

                    print(
                        f"Expanded text ad with ID {ad.id}, status "
                        f'"{row.ad_group_ad.status.name}", and headline '
                        f'"{expanded_text_ad_info.headline_part1}" - '
                        f'"{expanded_text_ad_info.headline_part2}" was '
                        f"found in ad group with ID {row.ad_group.id}."
                    )
                    result.append({'ad_id': ad.id,
                                   'ad_group_ad_name': row.ad_group_ad.status.name,
                                   'headline_part1': expanded_text_ad_info.headline_part1,
                                   'headline_part2': expanded_text_ad_info.headline_part2,
                                   'ad_group_id': row.ad_group.id
                                   })
            return result

        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def get_responsive_search_ads(self, customer_id, page_size=1000, ad_group_id=None):
        """
        Получить адаптивные поисковые объявления
        """
        def ad_text_assets_to_strs(assets):
            """Converts a list of AdTextAssets to a list of user-friendly strings."""
            s = []
            for asset in assets:
                s.append(f"\t {asset.text} pinned to {asset.pinned_field.name}")
            return s

        def ad_text_assets_to_dicts(assets):
            """
            Convert a list of AdTextAssets to a dict
            """
            return [{'text': txt.text,
                     'pinned_field': txt.pinned_field.name,
                     'asset_performance_label': txt.asset_performance_label.name,
                     'policy_summary_info_review_status': txt.policy_summary_info.review_status.name}
                    for txt in assets]

        try:
            ga_service = self.client.get_service("GoogleAdsService")
            query = '''
                SELECT ad_group.id, ad_group_ad.ad.id,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions,
                ad_group_ad.status FROM ad_group_ad
                WHERE ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.status != "REMOVED"'''

            # Optional: Specify an ad group ID to restrict search to only a given
            # ad group.
            if ad_group_id:
                query += f" AND ad_group.id = {ad_group_id}"

            ga_search_request = self.client.get_type("SearchGoogleAdsRequest")
            ga_search_request.customer_id = customer_id
            ga_search_request.query = query
            ga_search_request.page_size = page_size
            results = ga_service.search(request=ga_search_request)

            one_found = False
            res = []
            for row in results:
                one_found = True
                ad = row.ad_group_ad.ad
                print("Responsive search ad with resource name "
                      f'"{ad.resource_name}", status {row.ad_group_ad.status.name} '
                      "was found.")
                headlines = "\n".join(ad_text_assets_to_strs(ad.responsive_search_ad.headlines))
                descriptions = "\n".join(ad_text_assets_to_strs(ad.responsive_search_ad.descriptions))
                print(f"Headlines:\n{headlines}\nDescriptions:\n{descriptions}\n")
                res.append({'ad_name': ad.resource_name,
                            'ad_group_ad_status_name': row.ad_group_ad.status.name,
                            # 'headlines': [{'headline': ww.replace('\t', '').strip().split(' pinned to ')[0],
                            #                'pinned_to': ww.replace('\t', '').strip().split(' pinned to ')[1]}
                            #               for ww in ad_text_assets_to_strs(ad.responsive_search_ad.headlines)],
                            # 'descriptions': [{'description': ww.replace('\t', '').strip().split(' pinned to ')[0],
                            #                   'pinned_to': ww.replace('\t', '').strip().split(' pinned to ')[1]}
                            #                  for ww in ad_text_assets_to_strs(ad.responsive_search_ad.descriptions)],
                            'headlines': ad_text_assets_to_dicts(ad.responsive_search_ad.headlines),
                            'descriptions': ad_text_assets_to_dicts(ad.responsive_search_ad.descriptions)
                            })

            if not one_found:
                print("No responsive search ads were found.")
            return res

        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def pause_ad(self, customer_id, ad_group_id, ad_id):
        """
        Приостановить объявление
        """
        try:
            ad_group_ad_service = self.client.get_service("AdGroupAdService")
            ad_group_ad_operation = self.client.get_type("AdGroupAdOperation")

            ad_group_ad = ad_group_ad_operation.update
            ad_group_ad.resource_name = ad_group_ad_service.ad_group_ad_path(customer_id, ad_group_id, ad_id)
            ad_group_ad.status = self.client.enums.AdGroupStatusEnum.PAUSED
            self.client.copy_from(ad_group_ad_operation.update_mask,
                                  protobuf_helpers.field_mask(None, ad_group_ad._pb))

            ad_group_ad_response = ad_group_ad_service.mutate_ad_group_ads(customer_id=customer_id,
                                                                           operations=[ad_group_ad_operation])

            print(f"Paused ad group ad {ad_group_ad_response.results[0].resource_name}.")
            return 'OK'

        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def get_keyword_statistics(self, customer_id,
                               date_from,
                               date_to
                               ):
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""SELECT
                  customer.id,
                  campaign.id,
                  campaign.name,
                  ad_group.id,
                  ad_group.name,
                  ad_group_criterion.keyword.text,
                  ad_group_criterion.negative,
                  ad_group_criterion.system_serving_status,
                  ad_group_criterion.approval_status,
                  ad_group_criterion.keyword.match_type,
                  ad_group_criterion.final_urls,
                  bidding_strategy.name,
                  bidding_strategy.type,
                  metrics.average_cpm,
                  metrics.ctr,
                  metrics.clicks,
                  metrics.impressions,
                  metrics.average_cpc,
                  metrics.cost_per_all_conversions,
                  metrics.all_conversions,
                  metrics.all_conversions_value,
                  metrics.percent_new_visitors,
                  metrics.all_conversions_value_per_cost,
                  metrics.bounce_rate,
                  metrics.active_view_cpm,
                  metrics.average_cpe,
                  metrics.average_cpv,
                  metrics.active_view_ctr,
                  metrics.average_cost,
                  metrics.conversions,
                  metrics.conversions_by_conversion_date,
                  metrics.average_page_views,
                  metrics.interaction_rate,
                  metrics.interactions,
                  metrics.all_conversions_value_by_conversion_date,
                  metrics.conversions_value_by_conversion_date,
                  metrics.all_conversions_from_interactions_value_per_interaction,
                  metrics.average_time_on_site,
                  metrics.cost_micros,
                  metrics.engagement_rate,
                  metrics.engagements,
                  metrics.cost_per_conversion,
                  metrics.all_conversions_by_conversion_date
                FROM keyword_view
                WHERE segments.date > '{date_from}' AND segments.date < '{date_to}'
                AND ad_group_criterion.status != 'REMOVED'
                ORDER BY metrics.impressions DESC
                  """

            # Issues a search request using streaming.
            search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            stream = ga_service.search_stream(search_request)

            result = []
            for batch in stream:
                for row in batch.results:
                    # segments = row.segments
                    customer = row.customer
                    ad_group = row.ad_group
                    ad_group_criterion = row.ad_group_criterion
                    campaign = row.campaign
                    # keyword_view = row.keyword_view
                    bidding_strategy = row.bidding_strategy
                    metrics = row.metrics

                    print(row)
                    result.append({
                        # 'date': segments.date,
                        'customer_id': customer.id,
                        'campaign_id': campaign.id,
                        'campaign_name': campaign.name,
                        'ad_group_id': ad_group.id,
                        'ad_group_name': ad_group.name,
                        'keyword': ad_group_criterion.keyword.text,
                        'negative': ad_group_criterion.negative,
                        'system_serving_status': ad_group_criterion.system_serving_status.name,
                        'approval_status': ad_group_criterion.approval_status.name,
                        'match_type': ad_group_criterion.keyword.match_type.name,
                        'final_urls': [url.name for url in ad_group_criterion.final_urls],
                        'bidding_strategy_name': bidding_strategy.name,
                        'bidding_strategy_type': bidding_strategy.type.name,
                        'average_cpm': metrics.average_cpm,
                        'ctr': metrics.ctr,
                        'clicks': metrics.clicks,
                        'impressions': metrics.impressions,
                        'average_cpc': metrics.average_cpc,
                        'cost_per_all_conversions': metrics.cost_per_all_conversions,
                        'all_conversions': metrics.all_conversions,
                        'all_conversions_value': metrics.all_conversions_value,
                        'percent_new_visitors': metrics.percent_new_visitors,
                        'all_conversions_value_per_cost': metrics.all_conversions_value_per_cost,
                        'bounce_rate': metrics.bounce_rate,
                        'active_view_cpm': metrics.active_view_cpm,
                        'average_cpe': metrics.average_cpe,
                        'average_cpv': metrics.average_cpv,
                        'active_view_ctr': metrics.active_view_ctr,
                        'average_cost': metrics.average_cost,
                        'conversions': metrics.conversions,
                        'conversions_by_conversion_date': metrics.conversions_by_conversion_date,
                        'average_page_views': metrics.average_page_views,
                        'interaction_rate': metrics.interaction_rate,
                        'interactions': metrics.interactions,
                        'all_conversions_value_by_conversion_date': metrics.all_conversions_value_by_conversion_date,
                        'conversions_value_by_conversion_date': metrics.conversions_value_by_conversion_date,
                        'all_conversions_from_interactions_value_per_interaction':
                            metrics.all_conversions_from_interactions_value_per_interaction,
                        'average_time_on_site': metrics.average_time_on_site,
                        'cost_micros': metrics.cost_micros,
                        'engagement_rate': metrics.engagement_rate,
                        'engagements': metrics.engagements,
                        'cost_per_conversion': metrics.cost_per_conversion,
                        'all_conversions_by_conversion_date': metrics.all_conversions_by_conversion_date
                    })

            return result

        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            return None

    def get_campaign_statistics(self, customer_id,
                                date_from,
                                date_to
                                ):
        ga_service = self.client.get_service("GoogleAdsService")

        query = f"""
                SELECT
                campaign.name,
                metrics.clicks,
                segments.date
                FROM campaign
                WHERE segments.date > '{date_from}'
                    AND segments.date < '{date_to}'
                """

        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = customer_id
        search_request.query = query
        stream = ga_service.search_stream(search_request)

        result = []
        for batch in stream:
            for row in batch.results:
                campaign = row.campaign
                metrics = row.metrics
                segments = row.segments
                result.append({
                    'date': segments.date,
                    'name': campaign.name,
                    'clicks': metrics.clicks
                })

        return result















