from ..crawler.commoncrawl_extractor import CommonCrawlExtractor
import boto3
import botocore
import logging

class CustomCommonCrawlExtractor(CommonCrawlExtractor):
    # remote url where we can download the warc file
    __warc_path = None
    # download dir for warc files
    __local_download_dir_warc = './cc_download_warc/'
    # hosts (if None or empty list, any host is OK)
    __filter_valid_hosts = []  # example: ['elrancaguino.cl']
    # start date (if None, any date is OK as start date), as datetime
    __filter_start_date = None
    # end date (if None, any date is OK as end date)
    __filter_end_date = None
    # if date filtering is string, e.g., if we could not detect the date of an article, we will discard the article
    __filter_strict_date = True
    # language, if None, no language filtering is applied
    __filter_language = ["en"]
    # if language filtering is strict, e.g., if we could not detect the language of an article, we will discard the article
    __filter_strict_language = True
    # if True, the script checks whether a file has been downloaded already and uses that file instead of downloading
    # again. Note that there is no check whether the file has been downloaded completely or is valid!
    __reuse_previously_downloaded_files = True
    # continue after error
    __continue_after_error = False
    # ignore unicode errors
    __ignore_unicode_errors = False
    # fetch images
    __fetch_images = False
    # log level
    __log_level = logging.INFO
    __delete_warc_after_extraction = True
    __log_pathname_fully_extracted_warcs = None

    # commoncrawl.org
    __cc_base_url = 'https://data.commoncrawl.org/'
    __cc_bucket = 'commoncrawl'
    __cc_news_crawl_names = None

    # event handler called when an article was extracted successfully and passed all filter criteria
    __callback_on_article_extracted = None
    # event handler called when a warc file is fully processed
    __callback_on_warc_completed = None
    # if the download progress is shown
    __show_download_progress = False

    # logging
    logging.basicConfig(level=__log_level)
    __logger = logging.getLogger(__name__)

    def __get_language(self, warc_record, article):
        """
        Extracts the publishing date from the record
        :param warc_record:
        :return:
        """
        if hasattr(article, 'language'):
            return article.language
        else:
            return None
    
    # override filter_record to also filter by language
    def filter_record(self, warc_record, article=None):
        """
        Returns true if a record passes all tests: hosts, publishing date
        :param warc_record:
        :return: A tuple of (True or False) and an article (might be None)
        """
        # filter by host
        if self.__filter_valid_hosts:
            url = warc_record.rec_headers.get_header('WARC-Target-URI')

            # very simple check, check if one of the required host names is contained in the url of the WARC transaction
            # better would be to extract the host name from the WARC transaction Target URI and then check for equality
            # because currently something like g.co?forward_url=facebook.com would yield a positive filter test for
            # facebook.com even though the actual host is g.co
            for valid_host in self.__filter_valid_hosts:
                if valid_host in url:
                    break
            else:
                return False, article

        # filter by date
        if self.__filter_start_date or self.__filter_end_date:
            if not article:
                article = self._from_warc(warc_record)

            publishing_date = self.__get_publishing_date(warc_record, article)
            if not publishing_date:
                if self.__filter_strict_date:
                    return False, article
            else:  # here we for sure have a date
                # is article published too early?
                if self.__filter_start_date and publishing_date < self.__filter_start_date:
                    return False, article
                if self.__filter_end_date and publishing_date > self.__filter_end_date:
                    return False, article
                
        # filter by language
        if self.__filter_language:
            if not article:
                article = self._from_warc(warc_record)

            language = self.__get_language(warc_record, article)
            if not language:
                if self.__filter_strict_language:
                    return False, article
            else:  # here we for sure have a language
                for valid_language in self.__filter_language:
                    if valid_language in language:
                        break
                else:
                    return False, article

        return True, article
    