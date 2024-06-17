package com.joshlong.mogul.api.ai;

public enum ImageSize {

	SIZE_1024x1024(1024, 1024), SIZE_1024x1792(1024, 1792), SIZE_1792x1024(1792, 1024);

	private final int width, height;

	ImageSize(int width, int height) {
		this.width = width;
		this.height = height;
	}

	public int width() {
		return this.width;
	}

	public int height() {
		return this.height;
	}

}
