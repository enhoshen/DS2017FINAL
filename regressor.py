import os
import matplotlib.pyplot as plt
import numpy as np
from sklearn.svm import SVR
from sklearn.neural_network import MLPRegressor
from sklearn import datasets, linear_model
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from datacollector import *

def regression_models():
	regr = linear_model.LinearRegression( normalize= True)
	svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.1)
	svr_lin = SVR(kernel='linear', C=1e3)
	svr_poly = SVR(kernel='poly', C=1e3, degree=2)
	est = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1,
                                    max_depth=1, random_state=0, loss='ls')
	randforst = RandomForestRegressor(max_depth=2, random_state=0)
	nn = MLPRegressor(
    hidden_layer_sizes=(128,),  activation='relu', solver='adam', alpha=0.001, batch_size='auto',
    learning_rate='constant', learning_rate_init=0.01, power_t=0.5, max_iter=1000, shuffle=True,
    random_state=9, tol=0.0001, verbose=False, warm_start=False, momentum=0.9, nesterovs_momentum=True,
    early_stopping=False, validation_fraction=0.1, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
	return nn

def data_preprocess(data):
    data =  data / 255.0
    std_data = np.std(data.flatten())
    avg_data = np.mean(data.flatten()) 
    return (data-avg_data) / (std_data + 1e-8)

def save_model(model, save_dir, model_name, Settings):
    model_path = os.path.join(save_dir, model_name + '.h5')
    info_path = os.path.join(save_dir, model_name +'_info.json')
    model_json_path = os.path.join(save_dir, model_name +'.json')
    model.save(model_path)    
    model_json = model.to_json()
    with open(model_json_path, "w") as json_file:
        json_file.write(model_json)
    print('Saved trained model')


    with open(info_path, "w") as json_file:
        json.dump(Settings, json_file, indent=1)

def evaluate(pred,tst_y):
    return np.sum( (tst_y-pred)**2)/len(pred)
if __name__ =='__main__':
    # Load the diabetes dataset
    dataset= task1()

    (trn_x, trn_y), (tst_x, tst_y) = dataset.load_data('CY')
    # Create linear regression object
    regr = regression_models()

    # Train the model using the training sets
    regr.fit(trn_x, trn_y)

    # Make predictions using the testing set
    pred_y = regr.predict(tst_x)

    score = evaluate(pred_y, tst_y)
    print( score)
    plt.scatter ( tst_y,pred_y  , color = 'black')
    plt.ylabel('predicted frequency')
    plt.xlabel('true frequency')
    plt.show()
    # saving model information
    notes = 'record the training detail...'

    save_dir = dataset.dir
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    model_name = 'model1'
    save_model(regr, save_dir, model_name, Settings)
