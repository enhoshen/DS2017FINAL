import os
import matplotlib.pyplot as plt
import numpy as np
from sklearn.svm import SVR
from sklearn.neural_network import MLPRegressor
from sklearn import datasets, linear_model
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

def regression_models()
	regr = linear_model.LinearRegression()
	svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.1)
	svr_lin = SVR(kernel='linear', C=1e3)
	svr_poly = SVR(kernel='poly', C=1e3, degree=2)
	est = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1,
                                    max_depth=1, random_state=0, loss='ls')
	regr = RandomForestRegressor(max_depth=2, random_state=0)
	nn = MLPRegressor(
    hidden_layer_sizes=(10,),  activation='relu', solver='adam', alpha=0.001, batch_size='auto',
    learning_rate='constant', learning_rate_init=0.01, power_t=0.5, max_iter=1000, shuffle=True,
    random_state=9, tol=0.0001, verbose=False, warm_start=False, momentum=0.9, nesterovs_momentum=True,
    early_stopping=False, validation_fraction=0.1, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
	return regr

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


if __name__ =='__main__':
# Load the diabetes dataset
(diabetes_X_train, diabetes_y_train), (diabetes_X_test, diabetes_y_test) = dataset.load_data()


# maybe try to normalize the input first...
# normalization....(ref: data_preprocess function...)

# Create linear regression object
regr = regression_model()

# Train the model using the training sets
regr.fit(diabetes_X_train, diabetes_y_train)

# Make predictions using the testing set
diabetes_y_pred = regr.predict(diabetes_X_test)
score = regr.evaluate(diabetes_X_test, diabetes_y_test, verbose=0)

# saving model information
notes = 'record the training detail...'
Settings = {'test set accuracy': score[1],
            'notes':notes}

save_dir = os.path.join('.', 'model_zoo')
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

model_name = 'model1'
save_model(regr, save_dir, model_name, Settings)
