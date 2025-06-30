import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import (mean_absolute_error, 
                           r2_score, 
                           mean_squared_error,
                           explained_variance_score,
                           max_error)
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import pickle
import json
import os
import math
from scipy import stats
from sklearn.inspection import PartialDependenceDisplay
from sklearn.inspection import partial_dependence
import warnings
import matplotlib

# Настройки
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)

# Установка стилей
try:
    plt.style.use('seaborn-v0_8')  # Для новых версий
except:
    sns.set_theme()  # Альтернативный вариант
    print("Используется стандартный стиль Seaborn")

sns.set_palette("husl")
os.environ['SQLALCHEMY_SILENCE_UBER_WARNING'] = '1'

# Параметры подключения к БД
DB_PARAMS = {
    'host': 'localhost',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'port': '5432'
}

def get_db_connection():
    """Создание подключения к PostgreSQL"""
    conn = psycopg2.connect(**DB_PARAMS)
    return conn

def get_sqlalchemy_engine():
    """Создание SQLAlchemy engine для pandas"""
    connection_string = f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    engine = create_engine(connection_string)
    return engine

def create_results_table(conn):
    """Создание таблицы для хранения результатов ML"""
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm.vacancies_ml_results (
            id SERIAL PRIMARY KEY,
            model_name VARCHAR(100) NOT NULL,
            model_version VARCHAR(50) NOT NULL,
            training_date TIMESTAMP NOT NULL,
            metrics JSONB NOT NULL,
            feature_importance JSONB NOT NULL,
            model_path VARCHAR(255) NOT NULL,
            best_params JSONB,
            plots_path VARCHAR(255),
            dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS dm.vacancies_predictions (
            id SERIAL PRIMARY KEY,
            vacancy_id VARCHAR(100) REFERENCES dm.vacancies(vacancy_id),
            predicted_salary NUMERIC(15, 2) NOT NULL,
            actual_salary NUMERIC(15, 2),
            prediction_date TIMESTAMP NOT NULL,
            model_version VARCHAR(50) NOT NULL,
            dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_vacancy_prediction UNIQUE (vacancy_id, model_version)
        );
        """)
    conn.commit()

def save_model_results(conn, model_name, model_version, metrics, feature_importance, model_path, best_params=None, plots_path=None):
    """Сохранение результатов обучения модели"""
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO dm.vacancies_ml_results (
            model_name, 
            model_version, 
            training_date, 
            metrics, 
            feature_importance,
            model_path,
            best_params,
            plots_path
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            model_name,
            model_version,
            datetime.now(),
            json.dumps(metrics),
            json.dumps(feature_importance),
            model_path,
            json.dumps(best_params) if best_params else None,
            plots_path
        ))
    conn.commit()

def save_predictions(conn, predictions_df):
    """Сохранение предсказаний в БД"""
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE dm.vacancies_predictions")
        
        columns = predictions_df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"""
            INSERT INTO dm.vacancies_predictions ({', '.join(columns)}) 
            VALUES ({placeholders})
        """
        
        data = [tuple(row) for row in predictions_df.to_numpy()]
        cursor.executemany(query, data)
        conn.commit()

def load_data(conn):
    """Загрузка данных из витрины dm.vacancies"""
    query = """
    SELECT 
        vacancy_id,
        professional_sphere_name,
        schedule_type,
        experience_requirements,
        company_business_size,
        region_name,
        education_type,
        salary_min,
        salary_max,
        date_published,
        CASE 
            WHEN professional_sphere_name IN ('Продажи', 'Закупки', 'Снабжение', 'Торговля') THEN 'Торговля и снабжение'
            WHEN professional_sphere_name IN ('Кадровая служба', 'Управление персоналом') THEN 'HR и управление персоналом'
            WHEN professional_sphere_name IN ('Сельское хозяйство', 'Экология', 'Ветеринария') THEN 'Сельское хозяйство и экология'
            WHEN professional_sphere_name = 'Домашний персонал' THEN 'Домашний персонал'
            WHEN professional_sphere_name IN ('Лесная промышленность', 'Деревообрабатывающая промышленность', 'Целлюлозно-бумажная промышленность') THEN 'Лесная и деревообрабатывающая промышленность'
            WHEN professional_sphere_name = 'Легкая промышленность' THEN 'Легкая промышленность'
            WHEN professional_sphere_name = 'Не указано' THEN 'Не указано'
            WHEN professional_sphere_name IN ('Туризм', 'Гостиницы', 'Рестораны') THEN 'Гостинично-ресторанный бизнес'
            WHEN professional_sphere_name IN ('Транспорт', 'Автобизнес', 'Логистика', 'Склад', 'ВЭД') THEN 'Транспорт и логистика'
            WHEN professional_sphere_name = 'Высший менеджмент' THEN 'Топ-менеджмент'
            WHEN professional_sphere_name IN ('Металлургия', 'Металлообработка') THEN 'Металлургия и металлообработка'
            WHEN professional_sphere_name IN ('Химическая промышленность', 'Нефтехимическая промышленность', 'Топливная промышленность') THEN 'Химическая и нефтехимическая промышленность'
            WHEN professional_sphere_name IN ('Консалтинг', 'Стратегическое развитие', 'Управление') THEN 'Консалтинг и управление'
            WHEN professional_sphere_name = 'Рабочие специальности' THEN 'Рабочие специальности'
            WHEN professional_sphere_name IN ('Строительство', 'Ремонт', 'Стройматериалы', 'Недвижимость') THEN 'Строительство и недвижимость'
            WHEN professional_sphere_name IN ('Развлечения', 'Мода', 'Шоу-бизнес') THEN 'Индустрия развлечений'
            WHEN professional_sphere_name IN ('Бухгалтерия', 'Налоги', 'Управленческий учет') THEN 'Финансы и учет'
            WHEN professional_sphere_name = 'Работы, не требующие квалификации' THEN 'Неквалифицированный труд'
            WHEN professional_sphere_name = 'Производство' THEN 'Общее производство'
            WHEN professional_sphere_name IN ('Информационные технологии', 'Телекоммуникации', 'Связь') THEN 'IT и телекоммуникации'
            WHEN professional_sphere_name IN ('Банки', 'Кредит', 'Страхование', 'Пенсионное обеспечение') THEN 'Финансовые услуги'
            WHEN professional_sphere_name IN ('Искусство', 'Культура и развлечения') THEN 'Культура и искусство'
            WHEN professional_sphere_name IN ('Образование', 'Наука') THEN 'Образование и наука'
            WHEN professional_sphere_name = 'Добывающая промышленность' THEN 'Добывающая промышленность'
            WHEN professional_sphere_name = 'Пищевая промышленность' THEN 'Пищевая промышленность'
            WHEN professional_sphere_name IN ('Маркетинг', 'Реклама', 'PR') THEN 'Маркетинг и реклама'
            WHEN professional_sphere_name = 'Юриспруденция' THEN 'Юридические услуги'
            WHEN professional_sphere_name IN ('Безопасность', 'Службы охраны') THEN 'Безопасность'
            WHEN professional_sphere_name IN ('Здравоохранение', 'Социальное обеспечение') THEN 'Здравоохранение'
            WHEN professional_sphere_name IN ('ЖКХ', 'Эксплуатация') THEN 'ЖКХ и эксплуатация'
            WHEN professional_sphere_name = 'Электроэнергетика' THEN 'Энергетика'
            WHEN professional_sphere_name IN ('Услуги населению', 'Сервисное обслуживание') THEN 'Сфера услуг'
            WHEN professional_sphere_name IN ('Спорт', 'Фитнес', 'Салоны красоты') THEN 'Спорт и красота'
            WHEN professional_sphere_name IN ('Государственная служба', 'Некоммерческие организации') THEN 'Госсектор и НКО'
            WHEN professional_sphere_name = 'Машиностроение' THEN 'Машиностроение'
            WHEN professional_sphere_name IN ('Административная работа', 'Секретариат', 'АХО') THEN 'Административный персонал'
            ELSE 'Другое'
        END as company_industry,
        CASE 
            WHEN company_business_size = 'MICRO' THEN 50
            WHEN company_business_size = 'SMALL' THEN 100
            WHEN company_business_size = 'MIDDLE' THEN 250
            WHEN company_business_size = 'BIG' THEN 500 
            ELSE 1000
        END as company_size
    FROM dm.vacancies
    WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    return df

def preprocess_data(df):
    """Предварительная обработка данных"""
    df['avg_salary'] = (df['salary_min'] + df['salary_max']) / 2
    vacancy_ids = df['vacancy_id']
    
    cat_cols = ['professional_sphere_name', 'schedule_type', 'experience_requirements',
               'company_business_size', 'region_name', 'education_type', 'company_industry']
    
    label_encoders = {}
    for col in cat_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])
        label_encoders[col] = le
    
    df['year'] = pd.to_datetime(df['date_published']).dt.year
    df['month'] = pd.to_datetime(df['date_published']).dt.month
    df['day_of_week'] = pd.to_datetime(df['date_published']).dt.dayofweek
    
    df = df.drop(['salary_min', 'salary_max', 'date_published', 'vacancy_id'], axis=1)
    
    return df, label_encoders, vacancy_ids

def train_model_with_gridsearch(X_train, y_train):
    """Обучение модели с подбором гиперпараметров через GridSearchCV"""
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'max_features': ['auto', 'sqrt']
    }
    
    rf = RandomForestRegressor(random_state=42)
    grid_search = GridSearchCV(
        estimator=rf,
        param_grid=param_grid,
        cv=5,
        n_jobs=-1,
        verbose=2,
        scoring='neg_mean_absolute_error'
    )
    grid_search.fit(X_train, y_train)
    return grid_search.best_estimator_, grid_search.best_params_

def create_evaluation_plots(model, X_test, y_test, y_pred, feature_names, plots_dir='./model_evaluation_plots'):
    """Создание и сохранение графиков оценки модели"""
    os.makedirs(plots_dir, exist_ok=True)
    
    # 1. Actual vs Predicted with regression line
    plt.figure(figsize=(12, 8))
    sns.regplot(x=y_test, y=y_pred, scatter_kws={'alpha':0.3}, line_kws={'color':'red'})
    slope, intercept, r_value, _, _ = stats.linregress(y_test, y_pred)
    plt.title(f'Actual vs Predicted Values\nR²: {r_value**2:.3f} | Slope: {slope:.3f} | Intercept: {intercept:.3f}')
    plt.xlabel('Actual Salary')
    plt.ylabel('Predicted Salary')
    plt.grid(True)
    plt.savefig(f'{plots_dir}/actual_vs_predicted.png', bbox_inches='tight', dpi=300)
    plt.close()
    
    # 2. Error distribution with normal curve
    errors = y_test - y_pred
    plt.figure(figsize=(12, 8))
    sns.histplot(errors, kde=True, bins=30, stat='density')
    mu, std = stats.norm.fit(errors)
    x = np.linspace(min(errors), max(errors), 100)
    plt.plot(x, stats.norm.pdf(x, mu, std), 'r', linewidth=2)
    plt.title(f'Error Distribution\nMean: {mu:.2f} | STD: {std:.2f} | Skew: {stats.skew(errors):.2f}')
    plt.xlabel('Prediction Error')
    plt.ylabel('Density')
    plt.grid(True)
    plt.savefig(f'{plots_dir}/error_distribution.png', bbox_inches='tight', dpi=300)
    plt.close()
    
    # 3. Feature importance
    feature_imp = pd.DataFrame({
        'feature': [feature_names[i] for i in np.argsort(model.feature_importances_)],
        'importance': np.sort(model.feature_importances_)
    }).tail(15)
    
    plt.figure(figsize=(12, 10))
    colors = plt.cm.viridis(np.linspace(0, 1, len(feature_imp)))
    bars = plt.barh(feature_imp['feature'], feature_imp['importance'], color=colors)
    for bar in bars:
        width = bar.get_width()
        plt.text(width * 1.01, bar.get_y() + bar.get_height()/2,
                f'{width:.3f}',
                va='center', ha='left')
    plt.title('Top 15 Feature Importance')
    plt.xlabel('Feature Importance')
    plt.ylabel('Feature')
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{plots_dir}/feature_importance.png', bbox_inches='tight', dpi=300)
    plt.close()
    
    # 4. Residual plot
    plt.figure(figsize=(12, 8))
    sns.residplot(x=y_pred, y=errors, lowess=True, 
                 scatter_kws={'alpha': 0.3},
                 line_kws={'color': 'red', 'lw': 2})
    plt.axhline(y=0, color='black', linestyle='--')
    plt.title('Residual Plot')
    plt.xlabel('Predicted Values')
    plt.ylabel('Residuals')
    plt.grid(True)
    plt.savefig(f'{plots_dir}/residuals_plot.png', bbox_inches='tight', dpi=300)
    plt.close()
    
    # 5. Target distribution with quantiles
    plt.figure(figsize=(12, 8))
    sns.kdeplot(y_test, label='Actual Values', color='blue', shade=True)
    sns.kdeplot(y_pred, label='Predicted Values', color='red', shade=True)
    for q in [0.25, 0.5, 0.75, 0.9]:
        plt.axvline(x=np.quantile(y_test, q), color='blue', linestyle=':', alpha=0.5)
        plt.axvline(x=np.quantile(y_pred, q), color='red', linestyle=':', alpha=0.5)
    plt.title('Actual vs Predicted Salary Distribution')
    plt.xlabel('Salary')
    plt.ylabel('Density')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{plots_dir}/target_distribution.png', bbox_inches='tight', dpi=300)
    plt.close()
    
    # 6. Cumulative error distribution
    plt.figure(figsize=(12, 8))
    sorted_errors = np.sort(np.abs(errors))
    cum_dist = np.arange(1, len(sorted_errors)+1) / len(sorted_errors)
    plt.plot(sorted_errors, cum_dist, linewidth=2)
    for threshold in [5000, 10000, 15000]:
        idx = np.searchsorted(sorted_errors, threshold)
        if idx < len(cum_dist):
            plt.axvline(x=threshold, color='red', linestyle='--', alpha=0.5)
            plt.text(threshold, cum_dist[idx], 
                    f'{cum_dist[idx]*100:.1f}% ≤ {threshold}',
                    ha='right', va='center')
    plt.title('Cumulative Absolute Error Distribution')
    plt.xlabel('Absolute Error')
    plt.ylabel('Fraction of Predictions')
    plt.grid(True)
    plt.savefig(f'{plots_dir}/cumulative_error.png', bbox_inches='tight', dpi=300)
    plt.close()
    
    # 7. Partial Dependence Plots
    try:
        top_features = np.argsort(model.feature_importances_)[-4:]
        plt.figure(figsize=(15, 12))
        PartialDependenceDisplay.from_estimator(
            model, 
            X_test, 
            features=top_features,
            feature_names=feature_names,
            grid_resolution=20,
            n_cols=2,
            n_jobs=-1
        )
        plt.suptitle('Partial Dependence Plots for Top Features', y=1.02)
        plt.tight_layout()
        plt.savefig(f'{plots_dir}/partial_dependencies.png', bbox_inches='tight', dpi=300)
        plt.close()
    except Exception as e:
        print(f"Could not create partial dependence plots: {e}")

def train_model(df):
    """Обучение модели машинного обучения"""
    X = df.drop('avg_salary', axis=1)
    y = df['avg_salary']
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    print("Начало подбора гиперпараметров...")
    model, best_params = train_model_with_gridsearch(X_train_scaled, y_train)
    print("Подбор гиперпараметров завершен. Лучшие параметры:", best_params)
    
    y_pred = model.predict(X_test_scaled)
    
    metrics = {
        'mae': mean_absolute_error(y_test, y_pred),
        'r2': r2_score(y_test, y_pred),
        'rmse': math.sqrt(mean_squared_error(y_test, y_pred)),
        'max_error': max_error(y_test, y_pred),
        'explained_variance': explained_variance_score(y_test, y_pred),
        'mean_relative_error': np.mean(np.abs((y_test - y_pred) / y_test)) * 100,
        'median_absolute_error': np.median(np.abs(y_test - y_pred))
    }
    
    print("\nModel Metrics:")
    for metric, value in metrics.items():
        print(f"{metric.upper()}: {value:.4f}")
    
    feature_importance = dict(zip(X.columns, model.feature_importances_))
    
    # Create evaluation plots
    plots_dir = './model_evaluation_plots'
    create_evaluation_plots(model, X_test_scaled, y_test, y_pred, X.columns, plots_dir)
    
    return model, scaler, metrics, feature_importance, best_params, plots_dir

def save_model_to_db(model, scaler, label_encoders, model_name, model_version, metrics, 
                    feature_importance, best_params, plots_dir, conn):
    """Сохранение модели и метаданных в БД"""
    model_path = f"./{model_name}_{model_version}.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump({
            'model': model,
            'scaler': scaler,
            'label_encoders': label_encoders
        }, f)
    
    save_model_results(
        conn, 
        model_name, 
        model_version, 
        metrics, 
        feature_importance, 
        model_path, 
        best_params,
        plots_dir
    )
    return model_path

def make_predictions(model, scaler, df, label_encoders, vacancy_ids):
    """Создание предсказаний для всех вакансий"""
    X = df.drop('avg_salary', axis=1)
    X_scaled = scaler.transform(X)
    predictions = model.predict(X_scaled)
    
    results_df = pd.DataFrame({
        'vacancy_id': vacancy_ids,
        'predicted_salary': predictions,
        'actual_salary': df['avg_salary'],
        'prediction_date': datetime.now(),
        'model_version': 'v1.0'
    })
    
    return results_df

def main():
    conn = get_db_connection()
    engine = get_sqlalchemy_engine()
    
    try:
        create_results_table(conn)
        print("Загрузка данных...")
        df = load_data(conn)
        
        print("Предварительная обработка данных...")
        df_processed, label_encoders, vacancy_ids = preprocess_data(df)
        
        print("Обучение модели...")
        model, scaler, metrics, feature_importance, best_params, plots_dir = train_model(df_processed)
        
        print("Сохранение модели...")
        model_path = save_model_to_db(
            model, scaler, label_encoders, 
            'salary_predictor', 'v1.0', 
            metrics, feature_importance,
            best_params, plots_dir,
            conn
        )
        
        print("Создание предсказаний...")
        predictions_df = make_predictions(model, scaler, df_processed, label_encoders, vacancy_ids)
        
        print("Сохранение предсказаний в БД...")
        save_predictions(conn, predictions_df)
        
        print("\nПроцесс завершен успешно!")
        print(f"Модель сохранена в: {model_path}")
        print(f"Графики оценки сохранены в: {plots_dir}")
        print("\nМетрики модели:")
        for metric, value in metrics.items():
            print(f"{metric.upper()}: {value:.4f}")
        
    except Exception as e:
        print(f"\nОшибка в процессе выполнения: {e}")
    finally:
        conn.close()
        engine.dispose()

if __name__ == "__main__":
    main()