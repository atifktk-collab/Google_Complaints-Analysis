# Contributing to Google Complaints Analysis

Thank you for considering contributing to this project! Here are some guidelines to help you get started.

## How to Contribute

### Reporting Bugs

If you find a bug, please open an issue with:
- A clear description of the bug
- Steps to reproduce
- Expected vs actual behavior
- Your environment (OS, Python version, etc.)

### Suggesting Enhancements

We welcome suggestions for improvements! Please open an issue with:
- A clear description of the enhancement
- Why it would be useful
- Possible implementation approach

### Pull Requests

1. Fork the repository
2. Create a new branch (`git checkout -b feature/your-feature-name`)
3. Make your changes
4. Write or update tests as needed
5. Ensure all tests pass (`pytest tests/`)
6. Commit your changes (`git commit -m 'Add some feature'`)
7. Push to your branch (`git push origin feature/your-feature-name`)
8. Open a Pull Request

## Development Setup

1. Clone the repository:
```bash
git clone https://github.com/atifktk-collab/Google_Complaints-Analysis.git
cd Google_Complaints-Analysis
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install development dependencies:
```bash
pip install pytest pytest-cov black flake8
```

## Code Style

- Follow PEP 8 style guidelines
- Use meaningful variable and function names
- Add docstrings to all functions and classes
- Keep functions focused and concise
- Add comments for complex logic

## Testing

- Write unit tests for new features
- Ensure all existing tests pass
- Aim for high test coverage

Run tests with:
```bash
pytest tests/
```

Check coverage with:
```bash
pytest --cov=src tests/
```

## Documentation

- Update README.md if adding new features
- Add docstrings to new functions/classes
- Update configuration documentation if needed

## Questions?

Feel free to open an issue with your question or reach out to the maintainers.

Thank you for contributing! 🎉

