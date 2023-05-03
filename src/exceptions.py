from src import JobStrategy


class SignInError(Exception):
    def __init__(self, server: str, username: str):
        self.server = server.split('https://')[-1]
        self.username = username
        self.msg = f"Signing in into server \'{self.server}\' failed for user \'{self.username}\'."

    def __str__(self):
        return self.msg


class InvalidJobStrategyError(Exception):
    def __init__(self, wrong_js: str):
        self.wrong_js = wrong_js
        comma_space = ', \n        '
        self.msg = f"\nJob strategy \'{self.wrong_js}\' is invalid. \n" \
                   f"Select one from \n        {comma_space.join(JobStrategy.member_names())}"

    def __str__(self):
        return self.msg
