# src/purple_swan/analytics/regime/flexible_regime_detector.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Tuple, Dict, List
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.mixture import GaussianMixture
from hmmlearn.hmm import GaussianHMM
@dataclass
class RegimeDetectionResult:
    """Generic regime detection output"""
    regime_timeseries: np.ndarray  # (T,) array of regime assignments
    regime_probabilities: np.ndarray  # (T, n_regimes) soft assignments
    n_regimes: int
    model: 'RegimeDetector'
    metadata: Dict  # Algorithm-specific metadata


class RegimeDetector(ABC):
    """Base class for any regime detection algorithm"""

    @abstractmethod
    def fit(self, data: pd.DataFrame) -> 'RegimeDetectionResult':
        pass

    @abstractmethod
    def predict(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """Returns (regime_assignments, regime_probabilities)"""
        pass


class GMMRegimeDetector(RegimeDetector):
    """
    Gaussian Mixture Model regime detection.
    Flexible, works with any number of regimes.
    """

    def __init__(self, n_regimes: int, random_state: int = 42):
        self.n_regimes = n_regimes
        self.gmm = GaussianMixture(n_components=n_regimes, random_state=random_state)
        self.is_fitted = False

    def fit(self, data: pd.DataFrame) -> RegimeDetectionResult:
        """
        Fit GMM to standardized data.

        Args:
            data: (T, n_features) DataFrame, any columns

        Returns:
            RegimeDetectionResult with regime assignments
        """
        # Standardize
        self.mean_ = data.mean()
        self.std_ = data.std()
        data_std = (data - self.mean_) / self.std_

        # Fit GMM
        self.gmm.fit(data_std.values)
        self.is_fitted = True

        # Get regime assignments and probabilities
        regimes = self.gmm.predict(data_std.values)
        probs = self.gmm.predict_proba(data_std.values)

        return RegimeDetectionResult(
            regime_timeseries=regimes,
            regime_probabilities=probs,
            n_regimes=self.n_regimes,
            model=self,
            metadata={
                'bic': self.gmm.bic(data_std.values),
                'aic': self.gmm.aic(data_std.values),
                'weights': self.gmm.weights_,
                'means': self.gmm.means_,
                'covariances': self.gmm.covariances_
            }
        )

    def predict(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        if not self.is_fitted:
            raise ValueError("Model not fitted. Call fit() first.")

        data_std = (data - self.mean_) / self.std_
        regimes = self.gmm.predict(data_std.values)
        probs = self.gmm.predict_proba(data_std.values)

        return regimes, probs


class HMMRegimeDetector(RegimeDetector):
    """
    Hidden Markov Model regime detection.
    Captures regime persistence and transitions.
    """

    def __init__(self, n_regimes: int, n_iter: int = 100):
        self.n_regimes = n_regimes
        self.n_iter = n_iter
        self.is_fitted = False

    def fit(self, data: pd.DataFrame) -> RegimeDetectionResult:
        """
        Fit HMM using Baum-Welch algorithm (EM).

        Args:
            data: (T, n_features) DataFrame
        """
        from hmmlearn.hmm import GaussianHMM

        # Standardize
        self.mean_ = data.mean()
        self.std_ = data.std()
        data_std = (data - self.mean_) / self.std_

        # Fit HMM
        self.hmm = GaussianHMM(
            n_components=self.n_regimes,
            covariance_type='full',
            n_iter=self.n_iter
        )
        self.hmm.fit(data_std.values)
        self.is_fitted = True

        # Get regime assignments and probabilities
        regimes = self.hmm.predict(data_std.values)
        hidden_states = self.hmm.predict_proba(data_std.values)

        return RegimeDetectionResult(
            regime_timeseries=regimes,
            regime_probabilities=hidden_states,
            n_regimes=self.n_regimes,
            model=self,
            metadata={
                'transition_matrix': self.hmm.transmat_,
                'means': self.hmm.means_,
                'covariances': self.hmm.covars_,
                'startprob': self.hmm.startprob_
            }
        )

    def predict(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        if not self.is_fitted:
            raise ValueError("Model not fitted. Call fit() first.")

        data_std = (data - self.mean_) / self.std_
        regimes = self.hmm.predict(data_std.values)
        probs = self.hmm.predict_proba(data_std.values)

        return regimes, probs


class KMeansRegimeDetector(RegimeDetector):
    """
    K-Means clustering for regime detection.
    Fast, simple, good for preliminary exploration.
    """

    def __init__(self, n_regimes: int, random_state: int = 42):
        self.n_regimes = n_regimes
        self.random_state = random_state
        self.is_fitted = False

    def fit(self, data: pd.DataFrame) -> RegimeDetectionResult:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        # Standardize
        scaler = StandardScaler()
        data_std = scaler.fit_transform(data.values)

        self.scaler = scaler

        # Fit K-Means
        kmeans = KMeans(n_clusters=self.n_regimes, random_state=self.random_state, n_init=10)
        regimes = kmeans.fit_predict(data_std)

        self.kmeans = kmeans
        self.is_fitted = True

        # Compute soft assignments (distances → probabilities)
        distances = kmeans.transform(data_std)
        probs = self._distances_to_probabilities(distances)

        return RegimeDetectionResult(
            regime_timeseries=regimes,
            regime_probabilities=probs,
            n_regimes=self.n_regimes,
            model=self,
            metadata={
                'centers': kmeans.cluster_centers_,
                'inertia': kmeans.inertia_
            }
        )

    def predict(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        if not self.is_fitted:
            raise ValueError("Model not fitted. Call fit() first.")

        data_std = self.scaler.transform(data.values)
        regimes = self.kmeans.predict(data_std)
        distances = self.kmeans.transform(data_std)
        probs = self._distances_to_probabilities(distances)

        return regimes, probs

    @staticmethod
    def _distances_to_probabilities(distances: np.ndarray) -> np.ndarray:
        """Convert distances to probabilities using softmax"""
        # Invert distances (closer = higher probability)
        inverted = 1.0 / (distances + 1e-10)
        probs = inverted / inverted.sum(axis=1, keepdims=True)
        return probs


class NeuralRegimeDetector(RegimeDetector):
    """
    Neural network with learnable regime embeddings.
    Most flexible for complex dynamics.
    """

    def __init__(self, n_regimes: int, n_features: int, hidden_dim: int = 64):
        self.n_regimes = n_regimes
        self.n_features = n_features
        self.model = nn.Sequential(
            nn.Linear(n_features, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, n_regimes)
        )
        self.is_fitted = False

    def fit(self, data: pd.DataFrame, epochs: int = 50, lr: float = 0.01) -> RegimeDetectionResult:
        """
        Train neural network to output regime logits.
        Loss encourages regime diversity (high entropy).
        """
        # Standardize
        self.mean_ = data.mean()
        self.std_ = data.std()
        data_std = torch.from_numpy((data - self.mean_).values / self.std_.values).float()

        optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)

        for epoch in range(epochs):
            optimizer.zero_grad()

            # Forward pass
            logits = self.model(data_std)
            probs = torch.softmax(logits, dim=-1)

            # Loss: maximize entropy (encourage regime diversity)
            entropy = -torch.sum(probs * torch.log(probs + 1e-8), dim=1).mean()
            loss = -entropy

            loss.backward()
            optimizer.step()

        # Get final regime assignments
        with torch.no_grad():
            logits = self.model(data_std)
            probs = torch.softmax(logits, dim=-1)
            regimes = torch.argmax(probs, dim=1).numpy()
            probs_np = probs.numpy()

        self.is_fitted = True

        return RegimeDetectionResult(
            regime_timeseries=regimes,
            regime_probabilities=probs_np,
            n_regimes=self.n_regimes,
            model=self,
            metadata={'final_entropy': float(-entropy.detach().numpy())}
        )

    def predict(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        if not self.is_fitted:
            raise ValueError("Model not fitted. Call fit() first.")

        data_std = torch.from_numpy((data - self.mean_).values / self.std_.values).float()

        with torch.no_grad():
            logits = self.model(data_std)
            probs = torch.softmax(logits, dim=-1)
            regimes = torch.argmax(probs, dim=1).numpy()

        return regimes, probs.numpy()