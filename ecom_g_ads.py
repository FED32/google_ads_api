import pandas as pd
import numpy as np
# import argparse
import sys
import uuid
import datetime
from google.api_core import protobuf_helpers
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.auth.exceptions import RefreshError


class GAdsEcomru:
    def __init__(self,
                 client_id: str,
                 client_secret: str,
                 developer_token: str,
                 login_customer_id: str,
                 refresh_token: str
                 ):

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
        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status '
                  f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            self.client = None
        except RefreshError as ex:
            self.client = None
            print(ex)
        except ValueError:
            self.client = None
            print('Ошибка при инициализации клиента')

    def get_campaigns(self,
                      customer_id: str,
                      show_removed: bool = False
                      ):
        """
        Возвращает информацию о кампаниях
        https://developers.google.com/google-ads/api/samples/get-campaigns?hl=ru
        """
        try:
            ga_service = self.client.get_service("GoogleAdsService")
            query = """
                SELECT
                  campaign.id,
                  campaign.name,
                  campaign.status,
                  campaign.start_date,
                  campaign.end_date,
                  campaign_budget.name,
                  campaign_budget.amount_micros,
                  campaign.bidding_strategy_type,
                  campaign.advertising_channel_type
                FROM campaign
                """
            if show_removed is False:
                query += f" WHERE campaign.status != REMOVED"

            stream = ga_service.search_stream(customer_id=customer_id, query=query)
            result = []
            for batch in stream:
                for row in batch.results:
                    result.append({'campaign_id': row.campaign.id,
                                   'campaign_name': row.campaign.name,
                                   'campaign_status': row.campaign.status.name,
                                   'start_date': row.campaign.start_date,
                                   'end_date': row.campaign.end_date,
                                   'campaign_budget_name': row.campaign_budget.name,
                                   'campaign_budget': row.campaign_budget.amount_micros,
                                   'bidding_strategy_type': row.campaign.bidding_strategy_type.name,
                                   'advertising_channel_type': row.campaign.advertising_channel_type.name
                                   })

            return {'result': result}

        except GoogleAdsException as ex:
            # print(f'Request with ID "{ex.request_id}" failed with status '
            #       f'"{ex.error.code().name}" and includes the following errors:')
            # for error in ex.failure.errors:
            #     print(f'\tError with message "{error.message}".')
            #     if error.location:
            #         for field_path_element in error.location.field_path_elements:
            #             print(f"\t\tOn field: {field_path_element.field_name}")
            return {'error': str(ex.error.code().name), 'message': [error.message for error in ex.failure.errors]}

    def get_ad_groups(self,
                      customer_id: str,
                      page_size: int = 1000,
                      campaign_id: int = None,
                      show_removed: bool =False
                      ):
        """
        Получить группы
        https://developers.google.com/google-ads/api/samples/get-ad-groups?hl=ru
        """
        try:
            ga_service = self.client.get_service("GoogleAdsService")
            query = """
                SELECT
                  campaign.id,
                  ad_group.id,
                  ad_group.name,
                  ad_group.status,
                  ad_group.type
                FROM ad_group
                """
            if campaign_id is not None and show_removed is True:
                query += f" WHERE campaign.id = {campaign_id}"
            elif campaign_id is not None and show_removed is False:
                query += f" WHERE campaign.id = {campaign_id} AND ad_group.status != REMOVED"
            elif campaign_id is None and show_removed is False:
                query += f" WHERE ad_group.status != REMOVED"

            search_request = self.client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            search_request.page_size = page_size
            results = ga_service.search(request=search_request)
            res=[]
            for row in results:
                # print(
                #     f"Ad group with ID {row.ad_group.id} and name "
                #     f'"{row.ad_group.name}" was found in campaign with '
                #     f"ID {row.campaign.id}.")
                res.append({'ad_group_id': row.ad_group.id,
                            'ad_group_name': row.ad_group.name,
                            'ad_group_status': row.ad_group.status.name,
                            'ad_group_type': row.ad_group.type.name,
                            'campaign_id': row.campaign.id
                            })

            return {'result': res}

        except GoogleAdsException as ex:
            # print(f'Request with ID "{ex.request_id}" failed with status '
            #       f'"{ex.error.code().name}" and includes the following errors:')
            # for error in ex.failure.errors:
            #     print(f'\tError with message "{error.message}".')
            #     if error.location:
            #         for field_path_element in error.location.field_path_elements:
            #             print(f"\t\tOn field: {field_path_element.field_name}")
            return {'error': str(ex.error.code().name), 'message': [error.message for error in ex.failure.errors]}

    def get_keywords(self,
                     customer_id: str,
                     omit_unselected_resource_names: bool = False,
                     ad_group_id: int = None,
                     page_size=1000
                     ):
        """
        Получить ключевые слова
        https://developers.google.com/google-ads/api/samples/get-keywords?hl=ru
        """
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = """
                SELECT
                  campaign.id,
                  ad_group.id,
                  ad_group_criterion.type,
                  ad_group_criterion.criterion_id,
                  ad_group_criterion.keyword.text,
                  ad_group_criterion.negative,
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

            res = []
            for row in results:
                ad_group = row.ad_group
                ad_group_criterion = row.ad_group_criterion
                keyword = row.ad_group_criterion.keyword

                if omit_unselected_resource_names:
                    resource_name_log_statement = ""
                else:
                    resource_name_log_statement = f"{ad_group.resource_name}"

                res.append({'keyword_text': keyword.text,
                            'negative': ad_group_criterion.negative,
                            'match_type': keyword.match_type.name,
                            'criteria_type': ad_group_criterion.type_.name,
                            'criterion_id': ad_group_criterion.criterion_id,
                            'ad_group_id': ad_group.id,
                            'campaign_id': row.campaign.id,
                            'resource_name_log_statement': resource_name_log_statement})

            return {'result': res}

        except GoogleAdsException as ex:
            # print(f'Request with ID "{ex.request_id}" failed with status '
            #       f'"{ex.error.code().name}" and includes the following errors:')
            # for error in ex.failure.errors:
            #     print(f'\tError with message "{error.message}".')
            #     if error.location:
            #         for field_path_element in error.location.field_path_elements:
            #             print(f"\t\tOn field: {field_path_element.field_name}")

            return {'error': str(ex.error.code().name), 'message': [error.message for error in ex.failure.errors]}

    def create_keyword_plan(self,
                            customer_id: int,
                            keyword_plan_name='Keyword plan for traffic estimate'
                            ):
        """
            Adds a keyword plan to the given customer account.
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
        res = []
        if len(keywords) == len(bids):
            for keyword, bid in zip(keywords, bids):
                operation = self.client.get_type("KeywordPlanAdGroupKeywordOperation")
                keyword_plan_ad_group_keyword1 = operation.create
                keyword_plan_ad_group_keyword1.text = keyword
                keyword_plan_ad_group_keyword1.cpc_bid_micros = int(bid)*1e6
                keyword_plan_ad_group_keyword1.match_type = self.client.enums.KeywordMatchTypeEnum.BROAD
                keyword_plan_ad_group_keyword1.keyword_plan_ad_group = plan_ad_group
                operations.append(operation)

            response = keyword_plan_ad_group_keyword_service.mutate_keyword_plan_ad_group_keywords(
                customer_id=customer_id,
                operations=operations
            )
            for result in response.results:
                print("Created keyword plan ad group keyword with resource name: "
                      f"{result.resource_name}")
                res.append(result.resource_name)
        else:
            print("Input data incorrect")
            return None

        return res

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

        response = keyword_plan_negative_keyword_service.mutate_keyword_plan_campaign_keywords(
            customer_id=customer_id,
            operations=operations
        )

        res = response.results[0].resource_name
        print("Created keyword plan campaign keyword with resource name: "
              f"{res}")

        return res

    def add_keyword_plan(self, customer_id: int,
                         keywords: list[str],
                         bids: list[int],
                         negative_keywords: list[str] = None,
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
            res = dict()

            keyword_plan = self.create_keyword_plan(customer_id, keyword_plan_name)
            keyword_plan_campaign = self.create_keyword_plan_campaign(customer_id, keyword_plan, language,
                                                                      keyword_plan_campaign_name)
            keyword_plan_ad_group = self.create_keyword_plan_ad_group(customer_id, keyword_plan_campaign,
                                                                      keyword_plan_ad_group_name)
            keyword_plan_ad_group_keywords = self.create_keyword_plan_ad_group_keywords(customer_id,
                                                                                        keyword_plan_ad_group,
                                                                                        keywords,
                                                                                        bids)
            res['keyword_plan'] = keyword_plan
            res['keyword_plan_campaign'] = keyword_plan_campaign
            res['keyword_plan_ad_group'] = keyword_plan_ad_group
            res['keyword_plan_ad_group_keywords'] = keyword_plan_ad_group_keywords

            if negative_keywords is not None:
                keyword_plan_negative_campaign_keywords = self.create_keyword_plan_negative_campaign_keywords(
                    customer_id,
                    keyword_plan_campaign,
                    negative_keywords
                )

                res['keyword_plan_negative_campaign_keywords'] = keyword_plan_negative_campaign_keywords

            return res

        except GoogleAdsException as ex:
            # print(f'Request with ID "{ex.request_id}" failed with status '
            #       f'"{ex.error.code().name}" and includes the following errors:')
            # for error in ex.failure.errors:
            #     print(f'\tError with message "{error.message}".')
            #     if error.location:
            #         for field_path_element in error.location.field_path_elements:
            #             print(f"\t\tOn field: {field_path_element.field_name}")
            return {'error': str(ex.error.code().name), 'message': [error.message for error in ex.failure.errors]}

    def create_ad_group(self,
                        customer_id: int,
                        campaign_id: int,
                        name: str,
                        cpc_bid: int
                        ):
        """
        Создает группу объявлений
        https://developers.google.com/google-ads/api/samples/add-ad-groups?hl=ru
        https://developers.google.com/google-ads/api/reference/rpc/v13/AdGroupTypeEnum.AdGroupType
        https://developers.google.com/google-ads/api/reference/rpc/v13/AdGroupCriterion#cpc_bid_micros
        """
        try:
            ad_group_service = self.client.get_service("AdGroupService")
            campaign_service = self.client.get_service("CampaignService")

            # Create ad group.
            ad_group_operation = self.client.get_type("AdGroupOperation")
            ad_group = ad_group_operation.create
            # ad_group.name = f"{name} {uuid.uuid4()}"
            ad_group.name = f"{name}"
            ad_group.status = self.client.enums.AdGroupStatusEnum.ENABLED
            ad_group.campaign = campaign_service.campaign_path(customer_id, campaign_id)
            ad_group.type_ = self.client.enums.AdGroupTypeEnum.SEARCH_STANDARD
            ad_group.cpc_bid_micros = int(cpc_bid)*1e6

            # Add the ad group.
            ad_group_response = ad_group_service.mutate_ad_groups(customer_id=customer_id,
                                                                  operations=[ad_group_operation])
            print(f"Created ad group {ad_group_response.results[0].resource_name}.")

            return {'result': ad_group_response.results[0].resource_name}

        except GoogleAdsException as ex:
            # print(
            #     f'Request with ID "{ex.request_id}" failed with status '
            #     f'"{ex.error.code().name}" and includes the following errors:'
            # )
            # for error in ex.failure.errors:
            #     print(f'\tError with message "{error.message}".')
            #     if error.location:
            #         for field_path_element in error.location.field_path_elements:
            #             print(f"\t\tOn field: {field_path_element.field_name}")
            return {'error': str(ex.error.code().name), 'message': [error.message for error in ex.failure.errors]}

    def add_campaign(self,
                     customer_id: int,
                     campaign_budget_name: str,
                     campaign_budget_amount: float,
                     campaign_name: str,
                     date_from: str = None,
                     date_to: str = None,
                     enhanced_cpc_enabled: bool = True,
                     target_google_search: bool = True,
                     target_search_network: bool = True,
                     target_partner_search_network: bool = False,
                     target_content_network: bool = True
                     ):
        """
        Добавить кампанию
        https://developers.google.com/google-ads/api/samples/add-campaigns?hl=ru
        https://developers.google.com/google-ads/api/fields/v13/campaign
        https://developers.google.com/google-ads/api/reference/rpc/v13/BudgetDeliveryMethodEnum.BudgetDeliveryMethod
        https://developers.google.com/google-ads/api/fields/v13/campaign_budget#campaign_budget.amount_micros
        https://developers.google.com/google-ads/api/reference/rpc/v13/CampaignStatusEnum.CampaignStatus
        """

        # def handle_googleads_exception(exception):
        #     print(f'Request with ID "{exception.request_id}" failed with status '
        #           f'"{exception.error.code().name}" and includes the following errors:')
        #     for error in exception.failure.errors:
        #         print(f'\tError with message "{error.message}".')
        #         if error.location:
        #             for field_path_element in error.location.field_path_elements:
        #                 print(f"\t\tOn field: {field_path_element.field_name}")
            # sys.exit(1)

        campaign_budget_service = self.client.get_service("CampaignBudgetService")
        campaign_service = self.client.get_service("CampaignService")

        # Create a budget, which can be shared by multiple campaigns.
        campaign_budget_operation = self.client.get_type("CampaignBudgetOperation")
        campaign_budget = campaign_budget_operation.create
        # campaign_budget.name = f"{campaign_budget_name} {uuid.uuid4()}"
        campaign_budget.name = f"{campaign_budget_name}"
        campaign_budget.delivery_method = self.client.enums.BudgetDeliveryMethodEnum.STANDARD
        campaign_budget.amount_micros = campaign_budget_amount*1e6

        try:
            # Add budget.
            campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(
                customer_id=customer_id, operations=[campaign_budget_operation])

            # Create campaign.
            campaign_operation = self.client.get_type("CampaignOperation")
            campaign = campaign_operation.create
            # campaign.name = f"{campaign_name} {uuid.uuid4()}"
            campaign.name = f"{campaign_name}"
            campaign.advertising_channel_type = self.client.enums.AdvertisingChannelTypeEnum.SEARCH

            # Recommendation: Set the campaign to PAUSED when creating it to prevent
            # the ads from immediately serving. Set to ENABLED once you've added
            # targeting and the ads are ready to serve.
            campaign.status = self.client.enums.CampaignStatusEnum.PAUSED

            # Set the bidding strategy and budget.
            campaign.manual_cpc.enhanced_cpc_enabled = enhanced_cpc_enabled
            # campaign.target_spend.cpc_bid_ceiling_micros = 15000000
            campaign.campaign_budget = campaign_budget_response.results[0].resource_name

            # Set the campaign network options.
            campaign.network_settings.target_google_search = target_google_search
            campaign.network_settings.target_search_network = target_search_network
            campaign.network_settings.target_partner_search_network = target_partner_search_network
            # Enable Display Expansion on Search campaigns. For more details see:
            # https://support.google.com/google-ads/answer/7193800
            campaign.network_settings.target_content_network = target_content_network

            if date_from is not None:
                # Optional: Set the start date.
                # start_time = datetime.date.today() + datetime.timedelta(days=1)
                start_time = datetime.datetime.strptime(date_from, '%Y-%m-%d').date()
                campaign.start_date = datetime.date.strftime(start_time, "%Y%m%d")

            if date_to is not None:
                # Optional: Set the end date.
                # end_time = start_time + datetime.timedelta(weeks=4)
                end_time = datetime.datetime.strptime(date_to, '%Y-%m-%d').date()
                campaign.end_date = datetime.date.strftime(end_time, "%Y%m%d")

            # Add the campaign.
            try:
                campaign_response = campaign_service.mutate_campaigns(customer_id=customer_id,
                                                                      operations=[campaign_operation])
                res = campaign_response.results[0].resource_name
                print(f"Created campaign {res}.")
                return {"result": res}
            except GoogleAdsException as ex:
                # handle_googleads_exception(ex)
                return {'error': str(ex.error.code().name), 'message': [error.message for error in ex.failure.errors]}

        except GoogleAdsException as ex:
            # handle_googleads_exception(ex)
            return {'error': str(ex.error.code().name), 'message': [error.message for error in ex.failure.errors]}

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
                    customer_id=customer_id, operations=[ad_group_ad_operation])

                for result in ad_group_ad_response.results:
                    print(f"Created responsive search ad with resource name "
                          f'"{result.resource_name}".')
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
                SELECT
                campaign.id, 
                ad_group.id, 
                ad_group_ad.ad.id,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions,
                ad_group_ad.status 
                FROM ad_group_ad
                WHERE ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.status != "REMOVED"
                '''

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
                res.append({
                    'campaign_id': row.campaign.id,
                    'ad_group_id': row.ad_group.id,
                    'ad_id': ad.id,
                    'ad_name': ad.resource_name,
                    'ad_group_ad_status': row.ad_group_ad.status.name,
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

    def remove_ad(self, customer_id, ad_group_id, ad_id):
        """
        Удалить объявление
        """
        try:
            ad_group_ad_service = self.client.get_service("AdGroupAdService")
            ad_group_ad_operation = self.client.get_type("AdGroupAdOperation")

            resource_name = ad_group_ad_service.ad_group_ad_path(customer_id, ad_group_id, ad_id)
            ad_group_ad_operation.remove = resource_name
            ad_group_ad_response = ad_group_ad_service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=[ad_group_ad_operation])
            print(f"Removed ad group ad {ad_group_ad_response.results[0].resource_name}.")
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

    def remove_ad_group(self, customer_id, ad_group_id):
        """
        Удалить группу объявлений
        """
        try:
            ad_group_service = self.client.get_service("AdGroupService")
            ad_group_operation = self.client.get_type("AdGroupOperation")

            resource_name = ad_group_service.ad_group_path(customer_id, ad_group_id)
            ad_group_operation.remove = resource_name
            ad_group_response = ad_group_service.mutate_ad_groups(
                customer_id=customer_id,
                operations=[ad_group_operation])
            print(f"Removed ad group {ad_group_response.results[0].resource_name}.")
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

    def remove_campaign(self, customer_id, campaign_id):
        """
        Удалить кампанию
        """
        try:
            campaign_service = self.client.get_service("CampaignService")
            campaign_operation = self.client.get_type("CampaignOperation")

            resource_name = campaign_service.campaign_path(customer_id, campaign_id)
            campaign_operation.remove = resource_name
            campaign_response = campaign_service.mutate_campaigns(
                customer_id=customer_id,
                operations=[campaign_operation])
            print(f"Removed campaign {campaign_response.results[0].resource_name}.")
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

    def remove_keyword(self, customer_id, ad_group_id, criterion_id):
        """
        Удалить ключевое слово
        """
        try:
            agc_service = self.client.get_service("AdGroupCriterionService")
            agc_operation = self.client.get_type("AdGroupCriterionOperation")

            resource_name = agc_service.ad_group_criterion_path(customer_id, ad_group_id, criterion_id)
            agc_operation.remove = resource_name
            agc_response = agc_service.mutate_ad_group_criteria(
                customer_id=customer_id,
                operations=[agc_operation])
            print(f"Removed keyword {agc_response.results[0].resource_name}.")
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

    def update_ad_group(self, customer_id, ad_group_id, cpc_bid_micro_amount, enable=False):
        """
        Обновить группу
        """
        try:
            ad_group_service = self.client.get_service("AdGroupService")

            # Create ad group operation.
            ad_group_operation = self.client.get_type("AdGroupOperation")
            ad_group = ad_group_operation.update
            ad_group.resource_name = ad_group_service.ad_group_path(customer_id, ad_group_id)

            if enable is True:
                ad_group.status = self.client.enums.AdGroupStatusEnum.ENABLED
            else:
                ad_group.status = self.client.enums.AdGroupStatusEnum.PAUSED

            ad_group.cpc_bid_micros = cpc_bid_micro_amount*1e6
            self.client.copy_from(ad_group_operation.update_mask,
                                  protobuf_helpers.field_mask(None, ad_group._pb))

            # Update the ad group.
            ad_group_response = ad_group_service.mutate_ad_groups(customer_id=customer_id,
                                                                  operations=[ad_group_operation])
            print(f"Updated ad group {ad_group_response.results[0].resource_name}.")
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

    def update_campaign(self, customer_id,
                        campaign_id,
                        enable=False,
                        # date_from=None,
                        date_to=None,
                        target_google_search=None,
                        target_search_network=None,
                        target_partner_search_network=None,
                        target_content_network=None
                        ):
        """
        Обновить кампанию
        """
        try:
            campaign_service = self.client.get_service("CampaignService")
            # Create campaign operation.
            campaign_operation = self.client.get_type("CampaignOperation")
            campaign = campaign_operation.update
            campaign.resource_name = campaign_service.campaign_path(customer_id, campaign_id)

            if enable is True:
                campaign.status = self.client.enums.CampaignStatusEnum.ENABLED
            else:
                campaign.status = self.client.enums.CampaignStatusEnum.PAUSED

            # if date_from is not None:
            #     start_time = datetime.datetime.strptime(date_from, '%Y-%m-%d').date()
            #     campaign.start_date = datetime.date.strftime(start_time, "%Y%m%d")

            if date_to is not None:
                end_time = datetime.datetime.strptime(date_to, '%Y-%m-%d').date()
                campaign.end_date = datetime.date.strftime(end_time, "%Y%m%d")

            # Set the campaign network options.
            if target_google_search is not None:
                campaign.network_settings.target_google_search = target_google_search
            if target_search_network is not None:
                campaign.network_settings.target_search_network = target_search_network
            if target_partner_search_network is not None:
                campaign.network_settings.target_partner_search_network = target_partner_search_network
            if target_content_network is not None:
                campaign.network_settings.target_content_network = target_content_network

            # Retrieve a FieldMask for the fields configured in the campaign.
            self.client.copy_from(campaign_operation.update_mask,
                                  protobuf_helpers.field_mask(None, campaign._pb))

            campaign_response = campaign_service.mutate_campaigns(
                customer_id=customer_id, operations=[campaign_operation])

            print(f"Updated campaign {campaign_response.results[0].resource_name}.")
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

    def get_account_hierarchy2(self, login_customer_id):
        googleads_service = self.client.get_service("GoogleAdsService")
        query = """
            SELECT
              customer_client.resource_name,
              customer_client.client_customer,
              customer_client.level,
              customer_client.manager,
              customer_client.descriptive_name,
              customer_client.currency_code,
              customer_client.time_zone,
              customer_client.id
            FROM customer_client
            """
        result = []
        response = googleads_service.search(customer_id=str(login_customer_id), query=query)
        for row in response:
            print(row)
            result.append({
                'resource_name': row.customer_client.resource_name,
                'client_customer': row.customer_client.client_customer,
                'level': row.customer_client.level,
                'manager': row.customer_client.manager,
                'id': row.customer_client.id,
                'descriptive_name': row.customer_client.descriptive_name,
                'currency_code': row.customer_client.currency_code,
                'time_zone': row.customer_client.time_zone
            })
        return result

    def get_account_hierarchy(self, login_customer_id=None):
        """Gets the account hierarchy of the given MCC and login customer ID.
        Args:
          login_customer_id: Optional manager account ID. If none provided, this
          method will instead list the accounts accessible from the
          authenticated Google Ads account.
        """

        def print_account_hierarchy(customer_client, customer_ids_to_child_accounts, depth):
            """Prints the specified account's hierarchy using recursion.
            Args:
              customer_client: The customer client whose info will be printed; its
              child accounts will be processed if it's a manager.
              customer_ids_to_child_accounts: A dictionary mapping customer IDs to
              child accounts.
              depth: The current integer depth we are printing from in the account
              hierarchy.
            """
            if depth == 0:
                print("Customer ID (Descriptive Name, Currency Code, Time Zone)")
            customer_id = customer_client.id
            print("-" * (depth * 2), end="")
            print(f"{customer_id} ({customer_client.descriptive_name}, "
                  f"{customer_client.currency_code}, "
                  f"{customer_client.time_zone})")
            res.append({'level': "-" * (depth * 2),
                        'customer_id': customer_id,
                        'descriptive_name': customer_client.descriptive_name,
                        'currency_code': customer_client.currency_code,
                        'time_zone': customer_client.time_zone
                        })
            # Recursively call this function for all child accounts of customer_client.
            if customer_id in customer_ids_to_child_accounts:
                for child_account in customer_ids_to_child_accounts[customer_id]:
                    print_account_hierarchy(child_account, customer_ids_to_child_accounts, depth + 1)

        # Gets instances of the GoogleAdsService and CustomerService clients.
        googleads_service = self.client.get_service("GoogleAdsService")
        customer_service = self.client.get_service("CustomerService")
        # A collection of customer IDs to handle.
        seed_customer_ids = []
        # Creates a query that retrieves all child accounts of the manager
        # specified in search calls below.
        query = """
            SELECT
              customer_client.client_customer,
              customer_client.level,
              customer_client.manager,
              customer_client.descriptive_name,
              customer_client.currency_code,
              customer_client.time_zone,
              customer_client.id
            FROM customer_client
            WHERE customer_client.level <= 1"""

        # If a Manager ID was provided in the customerId parameter, it will be
        # the only ID in the list. Otherwise, we will issue a request for all
        # customers accessible by this authenticated Google account.
        if login_customer_id is not None:
            seed_customer_ids = [login_customer_id]
        else:
            print("No manager ID is specified. The example will print the hierarchies of all accessible customer IDs.")
            for customer_resource_name in customer_resource_names:
                customer_id = googleads_service.parse_customer_path(customer_resource_name)["customer_id"]
                print(customer_id)
                seed_customer_ids.append(customer_id)

        for seed_customer_id in seed_customer_ids:
            # Performs a breadth-first search to build a Dictionary that maps
            # managers to their child accounts (customerIdsToChildAccounts).
            unprocessed_customer_ids = [seed_customer_id]
            customer_ids_to_child_accounts = dict()
            root_customer_client = None

            while unprocessed_customer_ids:
                customer_id = int(unprocessed_customer_ids.pop(0))
                response = googleads_service.search(customer_id=str(customer_id), query=query)
                # Iterates over all rows in all pages to get all customer
                # clients under the specified customer's hierarchy.
                for googleads_row in response:
                    customer_client = googleads_row.customer_client
                    # The customer client that with level 0 is the specified
                    # customer.
                    if customer_client.level == 0:
                        if root_customer_client is None:
                            root_customer_client = customer_client
                        continue
                    # For all level-1 (direct child) accounts that are a
                    # manager account, the above query will be run against them
                    # to create a Dictionary of managers mapped to their child
                    # accounts for printing the hierarchy afterwards.
                    if customer_id not in customer_ids_to_child_accounts:
                        customer_ids_to_child_accounts[customer_id] = []
                    customer_ids_to_child_accounts[customer_id].append(customer_client)
                    if customer_client.manager:
                        # A customer can be managed by multiple managers, so to
                        # prevent visiting the same customer many times, we
                        # need to check if it's already in the Dictionary.
                        if customer_client.id not in customer_ids_to_child_accounts and customer_client.level == 1:
                            unprocessed_customer_ids.append(customer_client.id)

            if root_customer_client is not None:
                print("The hierarchy of customer ID "
                      f"{root_customer_client.id} is printed below:")
                print_account_hierarchy(root_customer_client, customer_ids_to_child_accounts, 0)
            else:
                print(f"Customer ID {login_customer_id} is likely a test "
                      "account, so its customer client information cannot be "
                      "retrieved.")

