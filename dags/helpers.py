from datetime import datetime


def set_max_active_runs():
    hour = datetime.now().hour
    if 8 <= hour <= 11:
        # morning
        return 3
    elif 12 <= hour <= 17:
        # afternoon
        return 3
    elif 18 <= hour <= 22:
        # evening
        return 15
    else:
        # night
        return 15