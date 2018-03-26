import math
def distribution(x,n):
    """
    :param x: 抛硬币次数
    :param n: 抛硬币x次得到正面次数n
    :return: 抛硬币x次得到n次正面的概率
    """
    a = (1/2)**x
    denominator = math.factorial(n)*math.factorial((x-n))
    Numerator = math.factorial(x)
    b = Numerator/denominator
    Ex = a*b
    return Ex


