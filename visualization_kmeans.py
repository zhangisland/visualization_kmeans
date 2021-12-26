# Created by Zhang Siru at 2021/12/26 13:15 with PyCharm
# Feature: 绘制聚类结果散点图
# Python Version: 3.6
# SourceFile: ./output/*

import os
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def read_file(_files_path):
    """
    :param _files_path: 结果文件目录: /output/
    :return: 对应簇的点坐标 dict
    """
    _point = []  # 点坐标
    _points = []  # 点集
    _cluster_id = []  # 所属簇id
    _files = os.listdir(_files_path)
    for f_name in _files:
        print(f_name)
        fp = open(_files_path+f_name)
        iter_fp = iter(fp)
        for line in iter_fp:
            line = line.replace('\t', ' ').replace('\n', '')  # 原始line=['623.5954732954837', '-899.2264508340368\t4\n']
            line = line.split(" ")
            _point.append(float(line[0]))
            _point.append(float(line[1]))
            _points.append(_point)
            _point = []
            _cluster_id.append(int(float(line[2])))
        # 如果对单个文件执行画图，则line34-line40取消注释，并注释掉lin42-line46
        # _k = len(_cluster_id)  # 簇的数目
        # _k_cluster = defaultdict(list)
        #
        # for i in range(len(_points)):
        #     _k_cluster[_cluster_id[i]].append(_points[i])
        #
        # draw_graph(_k_cluster=_k_cluster, _points=_points, _f_name=f_name)

    _k = len(_cluster_id)  # 簇的数目
    _k_cluster = defaultdict(list)

    for i in range(len(_points)):
        _k_cluster[_cluster_id[i]].append(_points[i])

    # print(_k_cluster)
    # print(len(_k_cluster))

    return _k_cluster, _points


def random_color():
    import random
    # 随机生成RGB颜色
    color_arr = ['1','2','3','4','5','6','7','8','9','A','B','C','D','E','F']
    color = ""
    for i in range(6):
        color += color_arr[random.randint(0, 14)]
    return '#' + color


def draw_graph(_k_cluster, _points, _f_name='part-m-00000'):
    """
    将不同簇用不同颜色着色
    :param _k_cluster: 簇及对应的点
    :param _points: 点坐标
    :param _f_name: 数据文件名
    :return:
    """
    fig, ax = plt.subplots(1, 1)

    color = []
    for i in range(len(_k_cluster)+1):
        color.append(random_color())

    for cluster_id, pts in _k_cluster.items():
        _x = [_p[0] for _p in pts]
        _y = [_p[1] for _p in pts]

        ax.scatter(x=_x, y=_y, c=color[int(cluster_id)], label='cluster_'+str(cluster_id))

    ax.xaxis.set_major_locator(ticker.MultipleLocator(base=300))
    ax.yaxis.set_major_locator(ticker.MultipleLocator(base=100))
    ax.set_title('K=5, iteration=1000')
    plt.savefig('visualization-'+str(_f_name)+'.jpg')
    plt.show()


def draw_raw(f_name):
    fp = open(f_name)
    _x = []
    _y = []
    for line in fp:
        line = line.replace('\n', '').split(' ')
        _x.append(float(line[0]))
        _y.append(float(line[1]))
    print(_x)
    print(_y)
    plt.title('Scatter Diagram')
    plt.plot(_x, _y, 'go')
    plt.show()


if __name__ == '__main__':
    path = './output/'
    k_cluster, points = read_file(_files_path=path)
    draw_graph(_k_cluster=k_cluster, _points=points)

