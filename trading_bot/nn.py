import torch

_device = torch.device("cuda" if torch.cuda.is_available() else "cpu")