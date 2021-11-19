import math
import numpy as np

###
### Opgave 1
###
data = [(1, 1), (1.5, 2.0), (3.0, 4.0), (5.0, 7.0), (3.5, 5.0), (4.5, 5.0), (3.5, 4.5)]


def euc(a, b):
    return math.sqrt((b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2)


best_dist = -1
best_pair = None
for p in data:
    for q in data:
        if p != q:
            d = euc(p, q)
            # print(f'Euclidan distance of {p} and {q} is: {d}')
            if d > best_dist:
                best_dist = d
                best_pair = (p, q)

print(f'Best pair is {best_pair} with distance {best_dist}')

c0 = {'points': [], 'centroid': best_pair[0]}
c1 = {'points': [], 'centroid': best_pair[1]}


def place_in_cluster(clusters, point):
    best_d = math.inf
    best_c = None
    for c in clusters:
        d = euc(point, c['centroid'])
        if d < best_d:
            best_d = d
            best_c = c
    best_c['points'].append(point)


def calc_centroid(cluster):
    cluster['centroid'] = (np.average([p[0] for p in cluster['points']]), np.average([p[1] for p in cluster['points']]))


def place_points_in_clusters(points, clusters):
    for c in clusters:
        c['points'] = []
    for p in points:
        clusters.sort(key=lambda x: euc(x['centroid'], p))
        clusters[0]['points'].append(p)
        calc_centroid(clusters[0])
    for c in clusters:
        calc_centroid(c)
    return [c['centroid'] for c in clusters]


cs = [c['centroid'] for c in [c0, c1]]
ncs = place_points_in_clusters(data, [c0, c1])
while cs != ncs:
    cs = ncs
    ncs = place_points_in_clusters(data, [c0, c1])

print(f'Clusters are {c0} and {c1}')

###
### Opgave 2
###

data = [(2, 10), (2, 5), (8, 4), (5, 8), (7, 5), (6, 4), (1, 2), (4, 9)]


def get_neighbourhood(points, p, r):
    result = []
    for n in points:
        if euc(n, p) <= r:
            result.append(n)
    return result


def DBscan(points, r, minPts):
    points = [{'point': p, 'visited': False, 'neighbourhood': get_neighbourhood(points, p, r), 'cluster': -1} for p in
              points]
    clusters = []
    i = 0
    for p in points:
        if not p['visited']:
            p['visited'] = True
            if len(p['neighbourhood']) >= minPts:
                c = [p]
                p['cluster'] = i
                n = [f for f in points if f['point'] in p['neighbourhood']]
                while len(n) > 0:
                    pp = n.pop(0)
                    if not pp['visited']:
                        pp['visited'] = True
                        if len(pp['neighbourhood']) >= minPts:
                            n.extend([f for f in points if f['point'] is pp['neighbourhood']])
                    if pp['cluster'] < 0:
                        c.append(pp)
                        pp['cluster'] = i
                clusters.append(c)
                i += 1
            else:
                p['cluster'] = -2
    return clusters


clusters = DBscan(data, 2, 2)
print(clusters)

###
### Opgave 3
###

