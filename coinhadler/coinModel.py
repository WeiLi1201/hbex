"""
    模型训练
"""

import os
import json
import numpy as np
import random
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn2pmml.pipeline import PMMLPipeline
from sklearn_pandas import DataFrameMapper
from sklearn.preprocessing import Imputer
from sklearn2pmml.decoration import ContinuousDomain
from sklearn.metrics import average_precision_score
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib
from sklearn import model_selection
from sklearn.metrics import recall_score




def splitTrain(data, test_ratio):
    """
    data: 样本数据框
    test_ratio: 测试集占整体样本比例，0-1
    return: train_df, test_df
    """
    shuffled_indices = np.random.permutation(len(data))
    test_set_size = int(len(data) * test_ratio)
    test_indices = shuffled_indices[:test_set_size]
    train_indices = shuffled_indices[test_set_size:]
    return data.iloc[train_indices],data.iloc[test_indices]


class TrainCoinModel:
    """
    """
    def __init__(self, sample_df):
        """
        sample_df: 训练样本
        """
        self.sample_df = sample_df


    def bestModel(self, feature, label, ratio=0.2):
        """
        feature: list 特征 e.g ["trade_cnt", "sell_cnt", "buy_cnt", "trade_amt", "sell_amt", "buy_amt", "sellbuy_bill_rate", "sellbuy_amount_rate"]
        label:
        ratio:
        return:
        """
        print("Traing Model begin ..." + "\n")
        list_n_estimators = range(10, 100)                  # 参数：树的棵数
        list_criterion = ['gini', 'entropy']                # 参数：节点分裂函数 gini/entropy
        list_max_depth =range(3, 20)                        # 参数：最大树的深度
        list_max_features = ['auto', 'sqrt', 'log2']        # 参数：最大的特征选择
        list_min_samples_split = range(2, 10)               # 参数：节点分裂所需的最小样本数

        best_n_estimators = 0
        best_criterion = ''
        best_max_depth = 0
        best_max_features = ''
        best_min_samples_split = 0

        global_accuray = 0
        global_auc = 0
        global_recall = 0
        global_average_precision = 0

        # 交叉验证,
        X = self.sample_df[feature].astype(float)
        y = self.sample_df[label].astype(int)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = ratio, stratify=y)

        print("sample rate(train:test) => " + str(len(y_train)) + ":" + str(len(y_test)) + "\n")

        for i in range(1, 200):
            rd_n_estimators = random.choice(list_n_estimators)
            rd_criterion = random.choice(list_criterion)
            rd_max_depth = random.choice(list_max_depth)
            rd_max_features = random.choice(list_max_features)
            rd_min_samples_split = random.choice(list_min_samples_split)

            # 初始化模型
            rf = RandomForestClassifier(n_estimators=rd_n_estimators,
                                        criterion=rd_criterion,
                                        max_depth=rd_max_depth,
                                        max_features=rd_max_features,
                                        min_samples_split=rd_min_samples_split)

            crf = rf.fit(X_train, y_train)
            predict_Y = crf.predict(X_test)
            rf_accuray = crf.score(X_test, y_test)
            rf_auc = roc_auc_score(y_test, predict_Y)
            rf_recall = recall_score(y_test, predict_Y)
            average_precision = average_precision_score(y_test, predict_Y)
            print("RandomForest Tree Accuracy: " + str(round(rf_accuray, 6)) +
                                        ", AUC: " + str(round(rf_auc, 6)) +
                                        ", Average precision-recall score：" + str(round(average_precision, 6)) +
                                        ", Recall: " + str(round(rf_recall, 6)))

            if rf_recall > global_recall:
                global_accuray = rf_accuray
                global_auc = rf_auc
                global_recall = rf_recall
                global_average_precision = average_precision
                best_n_estimators = rd_n_estimators
                best_criterion = rd_criterion
                best_max_depth = rd_max_depth
                best_max_features = rd_max_features
                best_min_samples_split = rd_min_samples_split

        print("\n")
        print("="*50 + "\n")
        best_parameters = '{"n_estimators":' + str(best_n_estimators) + \
                                ',"criterion":"' + str(best_criterion) + \
                                '","max_depth":' + str(best_max_depth) + \
                                ',"max_features":"' + str(best_max_features) + \
                                '","min_samples_split":' + str(best_min_samples_split) + \
                            '}'

        print("Best parameters: " + best_parameters +"\n")
        print("Best Accuracy: " + str(round(global_accuray, 6)) +
              ", Best AUC: " + str(round(global_auc, 6)) +
              ", Best Recall: " + str(round(global_recall, 6)),
              ", Best Average_precision: " + str(round(global_average_precision, 6)) + "\n")
        print("="*50 + "\n")

        return json.loads(best_parameters)


    def model2PMML(self, savepath, feature, label,
                   best_n_estimators=10, best_criterion="gini", best_max_depth=None,
                   best_max_features='auto', best_min_samples_split=2, is_PMML=True):
        """
        savepath: PMML文件保存路径
        feature: e.g. ["trade_cnt", "buy_cnt", "sell_cnt", "trade_amt", "buy_amt", "sell_amt",
                  "sellbuy_bill_rate", "sellbuy_amount_rate"]
        label: 标签列
        best_n_estimators:
        best_criterion:
        best_max_depth:
        return:
        """
        path_exists = False
        if not os.path.exists(savepath):
            os.makedirs(savepath)
            path_exists = True
        if not os.path.isdir(savepath) and path_exists:
            raise ValueError("is not folder!")

        if is_PMML:
            pipeline = PMMLPipeline([
                ("mapper", DataFrameMapper([
                    (feature, [ContinuousDomain(), Imputer()])])),
                ("classifier", RandomForestClassifier(n_estimators=best_n_estimators,
                                                      criterion=best_criterion,
                                                      max_depth=best_max_depth,
                                                      max_features=best_max_features,
                                                      min_samples_split=best_min_samples_split))
            ])
            pipeline.fit(self.sample_df[feature],
                         self.sample_df[label])
            from sklearn2pmml import sklearn2pmml
            sklearn2pmml(pipeline, savepath + "/RFcoin_model.pmml", with_repr=True)
            return True
        else:
            return False


    def model2JOB(self, savepath, feature, label,
                  best_n_estimators=10, best_criterion="gini", best_max_depth=None,
                  best_max_features='auto', best_min_samples_split=2, is_dump=True):
        """
        savepath:
        feature:
        label:
        best_n_estimators:
        best_criterion:
        best_max_depth:
        is_dump:
        return:
        """
        # from sklearn.externals import joblib
        # 4-5
        # loaded_model = joblib.load(filename)
        # result_ = loaded_model.predict(feature)
        path_exists = False
        if not os.path.exists(savepath):
            os.makedirs(savepath)
            path_exists = True
        if not os.path.isdir(savepath) and path_exists:
            raise ValueError("is not folder!")

        if is_dump:
            X = self.sample_df[feature]
            y = self.sample_df[label]
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y)
            rf = RandomForestClassifier(n_estimators=best_n_estimators,
                                        criterion=best_criterion,
                                        max_depth=best_max_depth,
                                        max_features=best_max_features,
                                        min_samples_split=best_min_samples_split)
            crf = rf.fit(X_train, y_train)
            predict_Y = crf.predict(X_test)
            rf_accuray = crf.score(X_test, y_test)
            rf_auc = roc_auc_score(y_test, predict_Y)
            rf_recall = recall_score(y_test, predict_Y)
            average_precision = average_precision_score(y_test, predict_Y)
            metric_ = '{"accuray":' + str(round(rf_accuray, 6)) + \
                      ',"auc":' + str(round(rf_auc, 6)) + \
                      ',"recall":' + str(round(rf_recall, 6)) + \
                      ',"average_precision":' + str(round(average_precision, 6)) + '}'
            filename = savepath + '/RFcoin_model.sav'
            joblib.dump(crf, filename)
            return metric_
        else:
            return None


    def cossValid(self, X, y):
        """"""
        # colormap = plt.cm.RdBu
        # plt.figure(figsize=(14, 12))
        # plt.title('Pearson Correlation of Features', y=1.05, size=15)
        # sns.heatmap(train_sample.ix[:, [32,33,34,35,36,31]].astype(float).corr(), linewidths=0.1, vmax=1.0,
        #             square=True, cmap=colormap, linecolor='white', annot=True)
        train_sizes, train_loss, test_loss = model_selection.learning_curve(RandomForestClassifier(n_estimators=30,
                                                                                                   criterion='gini',
                                                                                                   max_depth=9),
                                                                            X, y, cv=5,
                                                                            scoring='neg_log_loss',
                                                                            train_sizes = [0.1, 0.25, 0.5, 0.75, 1])
        train_loss_mean = -np.mean(train_loss, axis=1)
        test_loss_mean = -np.mean(test_loss, axis=1)
        plt.plot(train_sizes, train_loss_mean, 'o-', color="r",
                 label="Training")
        plt.plot(train_sizes, test_loss_mean, 'o-', color="g",
                 label="Cross-validation")
        plt.xlabel("Training examples")
        plt.ylabel("Loss")
        plt.legend(loc="best")
        plt.show()






