import requests


class BearerTokenizer:
    def __init__(self):
        self.bearer_token = self.create_bearer_token()

    def create_bearer_token(self):
        # first request - get verification token and antiforgery header
        URL = "http://host.docker.internal:5105/Account/Login?ReturnUrl=%2Fconnect%2Fauthorize%2Fcallback%3Fresponse_type%3Dtoken%26client_id%3Dorderingswaggerui%26redirect_uri%3Dhttp%253A%252F%252Fhost.docker.internal%253A5102%252Fswagger%252Foauth2-redirect.html%26scope%3Dorders%26state%3DTW9uIE1hciAwNiAyMDIzIDE1OjI5OjA4IEdNVCswMjAwIChJc3JhZWwgU3RhbmRhcmQgVGltZSk%253D"
        # sending get request and saving the response as response object
        response = requests.get(url=URL)
        # parse verification token from response
        request_verification_token = self.parse_request_verification_token(str(response.content))
        # parse antiforgery header from response cookies
        cookies = response.cookies.get_dict()

        # second request get token url
        URL = "http://host.docker.internal:5105/Account/Login?ReturnUrl=%2Fconnect%2Fauthorize%2Fcallback%3Fresponse_type%3Dtoken%26client_id%3Dorderingswaggerui%26redirect_uri%3Dhttp%253A%252F%252Fhost.docker.internal%253A5102%252Fswagger%252Foauth2-redirect.html%26scope%3Dorders%26state%3DTW9uIE1hciAwNiAyMDIzIDE1OjI5OjA4IEdNVCswMjAwIChJc3JhZWwgU3RhbmRhcmQgVGltZSk%253D"
        body = "ReturnUrl=%2Fconnect%2Fauthorize%2Fcallback%3Fresponse_type%3Dtoken%26client_id%3Dorderingswaggerui%26redirect_uri%3Dhttp%253A%252F%252Fhost.docker.internal%253A5102%252Fswagger%252Foauth2-redirect.html%26scope%3Dorders%26state%3DTW9uIE1hciAwNiAyMDIzIDE1OjI5OjA4IEdNVCswMjAwIChJc3JhZWwgU3RhbmRhcmQgVGltZSk%253D&Username=alice&Password=Pass123%24&button=login&__RequestVerificationToken=" + request_verification_token + "&RememberLogin=false"
        response = requests.post(url=URL,
                                 data=body,
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'},
                                 cookies=cookies,
                                 allow_redirects=False)
        cookies.update(response.cookies.get_dict())

        # last request - get bearer token
        URL = "http://host.docker.internal:5105/connect/authorize/callback?response_type=token&client_id=orderingswaggerui&redirect_uri=http%3A%2F%2Fhost.docker.internal%3A5102%2Fswagger%2Foauth2-redirect.html&scope=orders&state=TW9uIE1hciAwNiAyMDIzIDE1OjI5OjA4IEdNVCswMjAwIChJc3JhZWwgU3RhbmRhcmQgVGltZSk%3D"
        response = requests.get(url=URL,
                                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                                cookies=cookies,
                                allow_redirects=False)

        return self.parse_bearer_token(str(response.headers))

    @staticmethod
    def parse_request_verification_token(body_response):
        start_pos = body_response.find('__RequestVerificationToken') + 49
        length = body_response.index('"', start_pos)
        return body_response[start_pos:length]

    @staticmethod
    def parse_bearer_token(body_response):
        start_pos = body_response.find('access_token=') + 13
        length = body_response.index('&', start_pos)
        return body_response[start_pos:length]
