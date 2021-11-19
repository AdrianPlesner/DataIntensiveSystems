###
### Opgave 1
###

data = [(1, 2, 'a'), (2, 1, 'a'), (2, 5, 'b'), (3, 3, 'b'), (3, 6, 'b'),
        (4, 5, 'b'), (5, 5, 'c'), (6, 3, 'b'), (6, 7, 'c')]


def nr_of_class_in_set(s, c):
    result = 0
    for e in s:
        if e[2] == c:
            result += 1
    return result


def classes_in_set(s):
    result = []
    for e in s:
        if e[2] not in result:
            result.append(e[2])
    return result


def gini(s):
    result = 1
    for c in classes_in_set(s):
        result -= (nr_of_class_in_set(s, c) / len(s)) ** 2
    return result


def split_set(s, att, pred):
    s1 = []
    s2 = []
    for e in s:
        if pred(e[att]):
            s1.append(e)
        else:
            s2.append(e)
    return s1, s2


def information_gain(s, ab):
    a = ab[0]
    b = ab[1]
    return gini(s) - (len(a) / len(s)) * gini(a) - (len(b) / len(s)) * gini(b)


splits_on_0 = [{'split_val': x, 'ig': information_gain(data, split_set(data, 0, lambda y: y < x))} for x in range(2, 7)]
splits_on_1 = [{'split_val': x, 'ig': information_gain(data, split_set(data, 1, lambda y: y < x))} for x in range(2, 8)]
print(f'Splits on attribute 0: \n{splits_on_0}')
print(f' Splits on attribute 1: \n{splits_on_1}')
print('Best split is on attribute 1 with < 3')
l11, l12 = split_set(data, 1, lambda x: x < 3)
print(f'set l11 has gini {gini(l11)}, set l12 has gini {gini(l12)}')

# l11 has gini 0 and is pure

splits_on_0 = [{'split_val': x, 'ig': information_gain(l12, split_set(l12, 0, lambda y: y < x))} for x in range(2, 7)]
splits_on_1 = [{'split_val': x, 'ig': information_gain(l12, split_set(l12, 1, lambda y: y < x))} for x in range(2, 8)]
print(f'Splits on attribute 0: \n{splits_on_0}')
print(f' Splits on attribute 1: \n{splits_on_1}')
print('best split is on attribute 0 with < 5')
l21, l22 = split_set(l12, 0, lambda x: x < 5)

print(f'set l21 has gini {gini(l21)}, set l22 has gini {gini(l22)}')

# l21 has gini 0 and is pure

splits_on_1 = [{'split_val': x, 'ig': information_gain(l22, split_set(l22, 1, lambda y: y < x))} for x in range(2, 8)]
splits_on_0 = [{'split_val': x, 'ig': information_gain(l22, split_set(l22, 0, lambda y: y < x))} for x in range(2, 8)]
print(f'Splits on attribute 0: \n{splits_on_0}')
print(f' Splits on attribute 1: \n{splits_on_1}')
print('Best split is on attribute 1 with < 5')
l221, l222 = split_set(l22, 1, lambda x: x < 5)

print(f'set l221 has gini {gini(l221)}, set l222 has gini {gini(l222)}')

# both sets have gini 0, we are done


###
### Opgave 2
###
import math

xa = [1, 2, 3.5, 5, 1.5, 4]
ya = [3, 2, 1, 4, 4, 2]
xb = [2, 3, 4, 3.5, 1, 2]
yb = [3, 0.5, 3, 2, 2.5, 1]
xu = [4, 1.5, 3]
yu = [1, 2.5, 4]

A = [{'x': x, 'y': y, 'class': 'A'} for (x, y) in zip(xa, ya)]
B = [{'x': x, 'y': y, 'class': 'B'} for (x, y) in zip(xb, yb)]
data = A
data.extend(B)

unknown = [{'x': x, 'y': y} for (x, y) in zip(xu, yu)]


def euclidean(a, b):
    return math.sqrt((b['x'] - a['x']) ** 2 + (b['y'] - a['y']) ** 2)


def manhattan(a, b):
    return (math.fabs(a['x'] - b['x'])) + (math.fabs(a['y'] - b['y']))


def knn(train, point, k, func):
    if func == 'euclidean':
        f = euclidean
    elif func == 'manhattan':
        f = manhattan
    else:
        return None
    train.sort(key=lambda x: f(x, point))

    return train[:k]


# Find KNN for each unknown point and assign class for K = 1, 2, 3
print("\n\n\n")
for f in ('euclidean', 'manhattan'):
    for p in unknown:
        for k in range(1, 4):
            neighbours = knn(data, p, k, 'euclidean')
            print(f'The {k} nearest neighbours of point {p} using {f} are:\n{neighbours}\n')
